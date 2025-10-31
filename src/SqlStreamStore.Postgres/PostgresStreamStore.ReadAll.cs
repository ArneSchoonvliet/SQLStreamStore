namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Data;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.Logging;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;

    partial class PostgresStreamStore
    {
        protected override async Task<ReadAllPage> ReadAllForwardsInternal(long fromPositionInclusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            var correlation = Guid.NewGuid();

            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;

            var (messages, maxAgeDict, transactionIdDict, isEnd) = await ReadAllForwards(fromPositionInclusive, maxCount, prefetch, correlation, cancellationToken).ConfigureAwait(false);

            if(messages.Count == 0)
            {
                Logger.Debug("'ReadAllForwardsInternal' | No messages found");
                return new ReadAllPage(fromPositionInclusive, fromPositionInclusive, isEnd, ReadDirection.Forward, readNext, Array.Empty<StreamMessage>());
            }

            Logger.DebugFormat("'ReadAllForwardsInternal' | {amountOfMessages} messages found | Correlation: {correlation}", messages.Count, correlation);
            
            // If GapHandlingSettings is null, then 'Gaps' are handled in another layer (the generic base)
            // See ReadAllForwards in the ReadonlyStreamStoreBase class
            if(_settings.GapHandlingSettings != null)
            {
                var r = await HandleGaps(messages, maxAgeDict, transactionIdDict, isEnd, fromPositionInclusive, prefetch, correlation, cancellationToken).ConfigureAwait(false);

                isEnd = r.IsEnd;
                messages = r.Messages;
                maxAgeDict = r.MaxAgeDict;
            }

            var filteredMessages = FilterExpired(messages, maxAgeDict);
            var nextPosition = filteredMessages[filteredMessages.Count - 1].Position + 1;

            Logger.DebugFormat("'ReadAllForwardsInternal' from '{fromPositionInclusive}' completed | {amountOfMessages} messages found | NextPosition: {nextPosition} | IsEnd: {isEnd} | Correlation: {correlation}", 
                fromPositionInclusive, messages.Count, nextPosition, isEnd, correlation);
            
            return new ReadAllPage(fromPositionInclusive, nextPosition, isEnd, ReadDirection.Forward, readNext, filteredMessages.ToArray());
        }

        /// <summary>
        /// Handles gap detection and resolution in the event stream caused by PostgreSQL's MVCC.
        /// When reading events sequentially, gaps can appear due to:
        /// - Active transactions that haven't committed yet (temporary gaps)
        /// - Rolled-back transactions (permanent gaps)
        /// This handler distinguishes between the two to ensure no events are missed.
        /// </summary>
        /// <remarks>
        /// The algorithm uses PostgreSQL's Xmin (oldest visible transaction) to determine
        /// if gaps are permanent. When uncertain, it polls until all potentially gap-filling
        /// transactions complete, then re-reads the range.
        /// </remarks>
        private async Task<(ReadOnlyCollection<StreamMessage> Messages, ReadOnlyDictionary<string, int> MaxAgeDict, bool IsEnd)> HandleGaps(
            ReadOnlyCollection<StreamMessage> messages,
            ReadOnlyDictionary<string, int> maxAgeDict,
            ReadOnlyDictionary<long, ulong> transactionIdDict,
            bool isEnd,
            long fromPositionInclusive,
            bool prefetch,
            Guid correlation,
            CancellationToken cancellationToken)
        {
            Logger.TraceFormat("'HandleGaps' initiated | Correlation: {correlation}", correlation);
            
            if(!HasGaps(messages, fromPositionInclusive))
            {
                Logger.DebugFormat("No gaps detected | Correlation: {correlation}", correlation);
                LogMessages(correlation, fromPositionInclusive, messages, transactionIdDict);

                return (messages, maxAgeDict, isEnd);
            }

            // Check if gaps are permanent (from rolled-back transactions) by comparing
            // transaction IDs against Xmin. If all our transactions have aged out of
            // the snapshot horizon, any gaps must be from rollbacks, not pending commits.
            if(await AreGapsPermanent(transactionIdDict, correlation, cancellationToken))
            {
                Logger.DebugFormat("Gap(s) detected but they are flagged as real ones | Correlation: {correlation}", correlation);
                LogMessages(correlation, fromPositionInclusive, messages, transactionIdDict);

                return (messages, maxAgeDict, isEnd);
            }

            // Gaps might be temporary - retrieve active transactions that could fill them
            var transactions = await ReadTransactions(correlation, cancellationToken).ConfigureAwait(false);
            Logger.DebugFormat("Gap(s) detected going to poll until transactions are completed | Correlation: {correlation}", correlation);
            LogMessages(correlation, fromPositionInclusive, messages, transactionIdDict, transactions);

            // Wait for all transactions that could fill the gaps to complete (commit or rollback)
            await PollUntilMessagesAreStable(transactions, correlation, cancellationToken).ConfigureAwait(false);
            Logger.DebugFormat("Gap(s) polling stopped | Correlation: {correlation}", correlation);

            // Re-read up to the original range to avoid processing new events
            // that arrived during polling. Example: if original read returned positions 
            // 1-10 with gap at 5, we re-read 1-10 only, even if position 11 now exists.
            var trustedMessages = await ReadTrustedMessages(fromPositionInclusive, messages[messages.Count - 1].Position, prefetch, correlation, cancellationToken).ConfigureAwait(false);
            Logger.DebugFormat("Messages are re-read and shouldn't have fake gaps anymore | Correlation: {correlation}", correlation);
            LogMessages(correlation, fromPositionInclusive, trustedMessages.Messages);

            return trustedMessages;
        }

        /// <summary>
        /// Checks if there are any gaps in the message sequence.
        /// Gaps are detected when:
        /// 1. The first message position doesn't match the expected starting position
        /// 2. Any two consecutive messages have non-sequential positions
        /// </summary>
        private static bool HasGaps(ReadOnlyCollection<StreamMessage> messages, long fromPositionInclusive)
        {
            if(messages.Count == 0)
                return false;

            if(messages[0].Position != fromPositionInclusive)
                return true;

            for(int i = 0; i < messages.Count - 1; i++)
            {
                var expectedNextPosition = messages[i].Position + 1;
                var actualPosition = messages[i + 1].Position;

                if(expectedNextPosition != actualPosition)
                    return true;
            }

            return false;
        }

        /// <summary>
        /// Determines if detected gaps are permanent (from rolled-back transactions) or temporary
        /// (from pending transactions) by checking if transaction IDs have aged out of PostgreSQL's
        /// snapshot visibility horizon (Xmin).
        /// </summary>
        /// <remarks>
        /// Due to race conditions in concurrent execution, an older transaction (lower transaction ID)
        /// can claim a sequence position after a newer transaction has already claimed a later position.
        /// 
        /// Example timeline:
        /// - Transaction 999 is assigned its ID but pauses before claiming a sequence position
        /// - Transaction 1000 is assigned its ID and immediately claims sequence position 5
        /// - Transaction 999 resumes and claims sequence position 6
        /// - Result: Transaction 999 (older) has position 6, Transaction 1000 (newer) has position 5
        /// 
        /// The safety buffer accounts for this race condition by ensuring we don't prematurely
        /// conclude gaps are permanent when older transactions with lower IDs might still fill
        /// earlier sequence positions. Only returns true when we're certain all transactions that
        /// could fill the gaps have aged out
        /// </remarks>
        private async Task<bool> AreGapsPermanent(ReadOnlyDictionary<long, ulong> transactionIdDict, Guid correlation, CancellationToken cancellationToken)
        {
            var xMin = await ReadXmin(correlation, cancellationToken).ConfigureAwait(false);

            var maximumTransactionId = transactionIdDict.Max(x => x.Value);
            var safetyBuffer = _settings.GapHandlingSettings.SafetyGap;

            // If all transactions we've seen have aged out of the snapshot,
            // any remaining gaps must be from rolled-back transactions
            var agedOut = maximumTransactionId + safetyBuffer < xMin;
            Logger.TraceFormat("'AreGapsPermanent': {agedOut} | MaximumTransactionId: {maximumTransactionId} | XMin: {xMin} | SafetyBuffer: {safetyBuffer} | Correlation: {correlation}", 
                agedOut, maximumTransactionId, xMin, safetyBuffer, correlation);
            
            return agedOut;
        }

        /// <summary>
        /// Polls until all transactions that could potentially fill gaps have completed.
        /// Uses a two-phase approach:
        /// 1. Wait for initial pending transactions to complete
        /// 2. Wait for Xmin to advance past those transactions to ensure visibility
        /// </summary>
        /// <remarks>
        /// Implements exponential backoff and timeout safeguards to prevent indefinite blocking.
        /// </remarks>
        private async Task PollUntilMessagesAreStable(ActiveTransactions transactions, Guid correlation, CancellationToken cancellationToken)
        {
            var count = 0;
            var delayTime = _settings.GapHandlingSettings.InitialPollingDelay;
            var mode = PollingMode.ActiveTransactions;
            var maximumTransactionId = transactions.MaxTransactionId;
            var sw = Stopwatch.StartNew();

            while(true)
            {
                if(delayTime > 0)
                {
                    Logger.TraceFormat("Delay 'PollUntilMessagesAreStable' for {delayTime}ms | Correlation: {correlation}", delayTime, correlation);
                    await Task.Delay(delayTime, cancellationToken).ConfigureAwait(false);
                }

                if(count > 0 && count % _settings.GapHandlingSettings.PollingBackoffIntervalCount == 0)
                {
                    delayTime += _settings.GapHandlingSettings.PollingBackoffIncrement;
                }
                
                // Phase 0: Early exit because there are no active transactions
                if(transactions.NoActiveTransactions)
                {
                    Logger.DebugFormat("There are no active transactions, no need to poll all gaps should already be stable | Correlation: {correlation}", correlation);
                    return;
                }

                // Phase 1: Wait for the initial set of active transactions to complete
                if(mode == PollingMode.ActiveTransactions)
                {
                    var activeTransactions = await ReadTransactions(correlation, cancellationToken).ConfigureAwait(false);
                    if(!transactions.SharesTransactionsWith(activeTransactions))
                    {
                        Logger.DebugFormat(
                            "All initial active transactions are completed | Correlation: {correlation} | Total Polling time: {totalTime}ms, InitialTransactions: {initialTransactions}, ActiveTransactions: {activeTransactions}",
                            correlation,
                            sw.ElapsedMilliseconds,
                            transactions.ToString(),
                            activeTransactions.ToString());
                        mode = PollingMode.PollXmin;
                    }
                    else
                    {
                        Logger.TraceFormat(
                            "Not all initial active transactions are completed yet, continue polling | Correlation: {correlation} | Total Polling time: {totalTime}ms, InitialTransactions: {initialTransactions}, ActiveTransactions: {activeTransactions}",
                            correlation,
                            sw.ElapsedMilliseconds,
                            transactions.ToString(),
                            activeTransactions.ToString());
                    }
                }

                // Phase 2: Wait for Xmin to advance past completed transactions to ensure
                // their effects (commits or rollbacks) are visible to subsequent reads
                if(mode == PollingMode.PollXmin)
                {
                    var xMin = await ReadXmin(correlation, cancellationToken).ConfigureAwait(false);
                    if(transactions.IsSnapshotTransactionHigher(xMin))
                    {
                        Logger.DebugFormat(
                            "xMin has passed the maximumTransactionId all gaps should be stable now | Correlation: {correlation} | Total Polling time: {totalTime}ms, xMin: {xMin}, maximumTransactionId: {transactionId}",
                            correlation,
                            sw.ElapsedMilliseconds,
                            xMin,
                            maximumTransactionId);
                        return;
                    }

                    // Xmin should normally advance once transactions complete. If it doesn't,
                    // there may be a long-running transaction holding back the snapshot horizon.
                    Logger.TraceFormat(
                        "xMin didn't pass the maximumTransactionId yet, continue polling | Correlation: {correlation} | Total Polling time: {totalTime}ms, xMin: {xMin}, maximumTransactionId: {transactionId}",
                        correlation,
                        sw.ElapsedMilliseconds,
                        xMin,
                        maximumTransactionId);
                }

                // Safety valve: if we've exceeded the skip time threshold, stop polling to avoid
                // blocking the subscription indefinitely. This means we may miss an event but prevents
                // deadlock scenarios from halting all event processing.
                if(sw.ElapsedMilliseconds >= _settings.GapHandlingSettings.SkipTime)
                {
                    Logger.ErrorFormat(
                        "Possible SKIPPED EVENT as we will stop polling until the messages are stable | Polling took too long (>= {skipTime}) | Correlation: {correlation} | Total Polling time: {totalTime}ms | Mode: {mode}",
                        _settings.GapHandlingSettings.SkipTime,
                        correlation,
                        sw.ElapsedMilliseconds,
                        mode);
                    return;
                }

                // Early warning system: log when polling is taking longer than expected to help
                // diagnose potential deadlocks, slow transactions
                if(sw.ElapsedMilliseconds >= _settings.GapHandlingSettings.MinimumWarnTime)
                {
                    Logger.WarnFormat(
                        "Possible DEADLOCK! Polling until the messages are stable is taking some time (>= {warnTime}) | Correlation: {correlation} | Total Polling time: {totalTime}ms | Mode {mode}",
                        _settings.GapHandlingSettings.MinimumWarnTime,
                        correlation,
                        sw.ElapsedMilliseconds,
                        mode);
                }

                count++;
            }
        }

        private async Task<ActiveTransactions> ReadTransactions(Guid correlation, CancellationToken cancellationToken)
        {
            Logger.TraceFormat("'ReadTransactions' initiated | Correlation: {correlation}", correlation);
            
            var transactions = new List<uint>();

            using(var connection = await OpenConnection(cancellationToken).ConfigureAwait(false))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            using(var command = BuildFunctionCommand(_schema.ReadTransactions, transaction, Parameters.Name(connection.Database)))
            using(var reader = await command
                      .ExecuteReaderAsync(cancellationToken)
                      .ConfigureAwait(false))
            {
                while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    // Life would have been easier if pg_stat_activity (backend_xid) would return xid8 instead of xid
                    transactions.Add(reader.GetFieldValue<uint>(0));
                }
            }

            return new ActiveTransactions(transactions);
        }

        private async Task<ulong> ReadXmin(Guid correlation, CancellationToken cancellationToken)
        {
            Logger.TraceFormat("'ReadXmin' initiated | Correlation: {correlation}", correlation);
            
            using(var connection = await OpenConnection(cancellationToken).ConfigureAwait(false))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            using(var command = BuildFunctionCommand(_schema.ReadXmin, transaction))
            {
                var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as ulong?;
                return result ?? 0;
            }
        }

        private async Task<(ReadOnlyCollection<StreamMessage> messages, ReadOnlyDictionary<string, int> maxAgeDict, ReadOnlyDictionary<long, ulong> transactionIdDict, bool isEnd)>
            ReadAllForwards(
                long fromPositionInclusive,
                int maxCount,
                bool prefetch,
                Guid correlation,
                CancellationToken cancellationToken)
        {
            Logger.TraceFormat("'ReadAllForwards' initiated | Correlation: {correlation}", correlation);

            var sw = Stopwatch.StartNew();

            using(var connection = await OpenConnection(cancellationToken).ConfigureAwait(false))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            {
                using(var command = BuildFunctionCommand(_schema.ReadAll,
                          transaction,
                          Parameters.Count(maxCount + 1),
                          Parameters.Position(fromPositionInclusive),
                          Parameters.ReadDirection(ReadDirection.Forward),
                          Parameters.Prefetch(prefetch)))
                using(var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false))
                {
                    var messages = new List<StreamMessage>();
                    var maxAgeDict = new Dictionary<string, int>();
                    var transactionIdDict = new Dictionary<long, ulong>();
                    var isEnd = true;

                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        if(messages.Count == maxCount)
                            isEnd = false;
                        else
                        {
                            var streamIdInfo = new StreamIdInfo(reader.GetString(0));
                            var (message, maxAge, transactionId) = await ReadAllStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch).ConfigureAwait(false);

                            transactionIdDict.Add(message.Position, transactionId);

                            if(maxAge.HasValue)
                                maxAgeDict.TryAdd(message.StreamId, maxAge.Value);

                            messages.Add(message);
                        }
                    }

                    Logger.TraceFormat(
                        "'ReadAllForwards' query took: {timeTaken}ms | Correlation: {correlation} | fromPositionInclusive: {fromPositionInclusive}, maxCount: {maxCount}, prefetch: {preFetch} | count: {messageCount}, isEnd: {isEnd}",
                        sw.ElapsedMilliseconds,
                        correlation,
                        fromPositionInclusive,
                        maxCount,
                        prefetch,
                        messages.Count,
                        isEnd);

                    return (messages.AsReadOnly(), new ReadOnlyDictionary<string, int>(maxAgeDict), new ReadOnlyDictionary<long, ulong>(transactionIdDict), isEnd);
                }
            }
        }

        private async Task<(ReadOnlyCollection<StreamMessage> Messages, ReadOnlyDictionary<string, int> MaxAgeDict, bool IsEnd)> ReadTrustedMessages(
            long fromPositionInclusive,
            long toPositionInclusive,
            bool prefetch,
            Guid correlation,
            CancellationToken cancellationToken)
        {
            Logger.TraceFormat("'ReadTrustedForward' initiated | Correlation: {correlation}", correlation);

            var sw = Stopwatch.StartNew();

            using(var connection = await OpenConnection(cancellationToken).ConfigureAwait(false))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            {
                using(var command = BuildFunctionCommand(_schema.ReadTrustedForward,
                          transaction,
                          Parameters.Position(fromPositionInclusive),
                          Parameters.Position(toPositionInclusive + 1),
                          Parameters.Prefetch(prefetch)))
                using(var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false))
                {
                    var messages = new List<StreamMessage>();
                    var maxAgeDict = new Dictionary<string, int>();
                    var isEnd = true;

                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var streamIdInfo = new StreamIdInfo(reader.GetString(0));
                        var (message, maxAge, _) = await ReadAllStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch).ConfigureAwait(false);

                        if(maxAge.HasValue)
                            maxAgeDict.TryAdd(message.StreamId, maxAge.Value);

                        if(message.Position > toPositionInclusive)
                            isEnd = false;
                        else
                            messages.Add(message);
                    }

                    Logger.TraceFormat(
                        "'ReadTrustedForward' query took: {timeTaken}ms | Correlation: {correlation} | fromPositionInclusive: {fromPositionInclusive}, toPositionInclusive: {toPositionInclusive}, prefetch: {preFetch}, isEnd: {isEnd}",
                        sw.ElapsedMilliseconds,
                        correlation,
                        fromPositionInclusive,
                        toPositionInclusive,
                        prefetch,
                        isEnd);

                    return (messages.AsReadOnly(), new ReadOnlyDictionary<string, int>(maxAgeDict), isEnd);
                }
            }
        }

        protected override async Task<ReadAllPage> ReadAllBackwardsInternal(
            long fromPositionInclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            var ordinal = fromPositionInclusive == Position.End ? long.MaxValue : fromPositionInclusive;

            using(var connection = await OpenConnection(cancellationToken).ConfigureAwait(false))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            {
                using(var command = BuildFunctionCommand(_schema.ReadAll,
                          transaction,
                          Parameters.Count(maxCount + 1),
                          Parameters.Position(ordinal),
                          Parameters.ReadDirection(ReadDirection.Backward),
                          Parameters.Prefetch(prefetch)))
                using(var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false))
                {
                    if(!reader.HasRows)
                    {
                        // When reading backwards and there are no more items, then next position is LongPosition.Start,
                        // regardless of what the fromPosition is.
                        return new ReadAllPage(Position.Start, Position.Start, true, ReadDirection.Backward, readNext, Array.Empty<StreamMessage>());
                    }

                    var messages = new List<StreamMessage>();
                    var maxAgeDict = new Dictionary<string, int>();

                    long lastOrdinal = 0;
                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var streamIdInfo = new StreamIdInfo(reader.GetString(0));
                        var (message, maxAge, _) = await ReadAllStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch).ConfigureAwait(false);

                        if(maxAge.HasValue)
                        {
                            if(!maxAgeDict.ContainsKey(message.StreamId))
                            {
                                maxAgeDict.Add(message.StreamId, maxAge.Value);
                            }
                        }

                        messages.Add(message);
                        lastOrdinal = message.Position;
                    }

                    bool isEnd = true;
                    var nextPosition = lastOrdinal;

                    if(messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    var filteredMessages = FilterExpired(messages.AsReadOnly(), new ReadOnlyDictionary<string, int>(maxAgeDict));

                    fromPositionInclusive = filteredMessages.Count > 0 ? filteredMessages[0].Position : 0;

                    return new ReadAllPage(fromPositionInclusive, nextPosition, isEnd, ReadDirection.Backward, readNext, filteredMessages.ToArray());
                }
            }
        }

        private async Task<(StreamMessage message, int? maxAge, ulong transactionId)> ReadAllStreamMessage(
            NpgsqlDataReader reader,
            PostgresqlStreamId streamId,
            bool prefetch)
        {
            async Task<string> ReadString(int ordinal)
            {
                if(reader.IsDBNull(ordinal))
                {
                    return null;
                }

                using(var textReader = await reader.GetTextReaderAsync(ordinal).ConfigureAwait(false))
                {
                    return await textReader.ReadToEndAsync().ConfigureAwait(false);
                }
            }

            var messageId = reader.GetGuid(1);
            var streamVersion = reader.GetInt32(2);
            var position = reader.GetInt64(3);
            var createdUtc = reader.GetDateTime(4);
            var type = reader.GetString(5);
            var transactionId = reader.GetFieldValue<ulong>(6);
            var jsonMetadata = await ReadString(7).ConfigureAwait(false);

            if(prefetch)
            {
                return (new StreamMessage(streamId.IdOriginal, messageId, streamVersion, position, createdUtc, type, jsonMetadata, await ReadString(8).ConfigureAwait(false)),
                    reader.GetFieldValue<int?>(9), transactionId);
            }

            return (new StreamMessage(streamId.IdOriginal, messageId, streamVersion, position, createdUtc, type, jsonMetadata, ct => GetJsonData(streamId, streamVersion)(ct)),
                reader.GetFieldValue<int?>(9), transactionId);
        }

        private void LogMessages(
            Guid correlation,
            long fromPositionInclusive,
            ReadOnlyCollection<StreamMessage> messages,
            ReadOnlyDictionary<long, ulong> transactionIdDict = null,
            ActiveTransactions activeTransactions = null)
        {
            if(!Logger.IsTraceEnabled()) return;

            var messagesLog = messages.Count == 0
                ? "No messages"
                : $"Count: {messages.Count} | " + string.Join(" | ",
                    messages.Select((x, i) =>
                        $"Position: {x.Position}, Array index: {i}" +
                        (transactionIdDict != null
                            ? $", Transaction id: {transactionIdDict[x.Position]}"
                            : "")));

            if(activeTransactions == null)
            {
                Logger.TraceFormat("Correlation: {0} | HasGaps: {1} | Messages: {2}",
                    correlation,
                    HasGaps(messages, fromPositionInclusive),
                    messagesLog);
            }
            else
            {
                Logger.TraceFormat("Correlation: {0} | HasGaps: {1} | Messages: {2} | ActiveTransactions: {3}",
                    correlation,
                    HasGaps(messages, fromPositionInclusive),
                    messagesLog,
                    activeTransactions.ToString());
            }
        }

        private enum PollingMode
        {
            ActiveTransactions = 1,
            PollXmin
        }
    }
}