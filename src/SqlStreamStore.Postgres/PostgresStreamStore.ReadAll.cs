namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Npgsql;
    using SqlStreamStore.Logging;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;

    partial class PostgresStreamStore
    {
        protected override async Task<ReadAllPage> ReadAllForwardsInternal(long fromPositionExclusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            var correlation = Guid.NewGuid();

            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;

            var (messages, maxAgeDict, transactionIdsInProgress) = await ReadAllForwards(fromPositionExclusive, maxCount, prefetch, correlation, cancellationToken);

            var isEnd = IsEnd(messages.Count, maxCount);
            if(!isEnd)
                messages.RemoveAt(maxCount);

            if(_settings.GapHandlingSettings != null)
            {
                var r = await HandleGaps(messages.ToImmutableArray(), maxAgeDict.ToImmutableDictionary(), transactionIdsInProgress, isEnd, fromPositionExclusive, maxCount, prefetch, correlation, cancellationToken);

                isEnd = r.isEnd;
                messages = r.messages;
            }

            if(!messages.Any())
            {
                return new ReadAllPage(fromPositionExclusive, fromPositionExclusive, isEnd, ReadDirection.Forward, readNext, Array.Empty<StreamMessage>());
            }

            var filteredMessages = FilterExpired(messages, maxAgeDict);

            var nextPosition = filteredMessages[filteredMessages.Count - 1].Position + 1;

            return new ReadAllPage(fromPositionExclusive, nextPosition, isEnd, ReadDirection.Forward, readNext, filteredMessages.ToArray());
        }

        private static bool IsEnd(int messageCount, int maxCount) => messageCount != maxCount + 1;

        private async Task<(ImmutableArray<StreamMessage> messages, ImmutableDictionary<string, int> maxAgeDict, bool isEnd)> HandleGaps(
            ImmutableArray<StreamMessage> messages,
            ImmutableDictionary<string, int> maxAgeDict,
            TxIdList transactionsInProgress,
            bool isEnd,
            long fromPositionInclusive,
            int maxCount,
            bool prefetch,
            Guid correlation,
            CancellationToken cancellationToken)
        {
            var hasMessages = messages.Length > 0;

            Logger.TraceFormat("{correlation} {messages} | Transactions in progress: {transactionsInProgress}",
                correlation,
                hasMessages ? $"Count: {messages.Length} | {string.Join("|", messages.Select((x, i) => $"Position: {x.Position} Array index: {i}"))}" : "No messages",
                transactionsInProgress);

            if(!hasMessages && !transactionsInProgress.Any())
            {
                Logger.TraceFormat("{correlation} No messages found, no transactions in progress. We will return empty list of messages with isEnd to true", correlation, messages);
                return (ImmutableArray<StreamMessage>.Empty, ImmutableDictionary<string, int>.Empty, true);
            }

            if(!(transactionsInProgress.Count > 0))
            {
                Logger.TraceFormat("{correlation} No transactions in progress, no need for gap checking", correlation);

                return (messages, maxAgeDict, isEnd);
            }

            Logger.TraceFormat("{correlation} Danger zone! We have messages & transactions in progress, we need to start gap checking", correlation);

            // Check for gap between last page and this. 
            if(messages[0].Position != fromPositionInclusive)
            {
                Logger.TraceFormat(
                    "{correlation} fromPositionInclusive {fromPositionInclusive} does not match first position of received messages {position} and transactions: {transactions} in progress",
                    correlation,
                    fromPositionInclusive,
                    transactionsInProgress);

                await PollTransactions(correlation, transactionsInProgress, cancellationToken);

                return await ReadTrustedMessages(fromPositionInclusive, messages[messages.Length - 1].Position, maxCount, prefetch, correlation, cancellationToken);
            }


            for(int i = 0; i < messages.Length - 1; i++)
            {
                var expectedNextPosition = messages[i].Position + 1;
                var actualPosition = messages[i + 1].Position;
                Logger.TraceFormat("{correlation} Gap checking. Expected position: {expectedNextPosition} | Actual position: {actualPosition}", correlation, expectedNextPosition, actualPosition);

                if(expectedNextPosition != actualPosition)
                {
                    Logger.TraceFormat("{correlation} Gap detected", correlation);

                    await PollTransactions(correlation, transactionsInProgress, cancellationToken);

                    return await ReadTrustedMessages(fromPositionInclusive, messages[messages.Length - 1].Position, maxCount, prefetch, correlation, cancellationToken);
                }
            }

            return (messages, maxAgeDict, isEnd);
        }

        private async Task PollTransactions(Guid correlation, TxIdList transactionsInProgress, CancellationToken cancellationToken)
        {
            Logger.TraceFormat("{correlation} Transactions {transactions} are in progress, so gaps might be filled, start comparing", correlation, transactionsInProgress);

            bool stillInProgress;
            int count = 1;
            int delayTime = 10;
            long totalTime = 0;

            var sw = Stopwatch.StartNew();
            do
            {
                stillInProgress = await ReadAnyTransactionsInProgress(transactionsInProgress, cancellationToken);
                Logger.TraceFormat("{correlation} Transactions still pending. Query 'ReadAnyTransactionsInProgress' took: {timeTaken}ms", correlation, sw.ElapsedMilliseconds);

                if(_settings.GapHandlingSettings.AllowSkip && totalTime > _settings.GapHandlingSettings.PossibleAllowedSkipTime)
                {
                    Logger.ErrorFormat(
                        "{correlation} Possible SKIPPED EVENT as we will stop waiting for in progress transactions! One of these transactions: {transactions} is in progress for longer then {totalTime}ms",
                        correlation,
                        transactionsInProgress,
                        _settings.GapHandlingSettings.PossibleAllowedSkipTime);
                    return;
                }

                if(totalTime > _settings.GapHandlingSettings.PossibleDeadlockTime)
                {
                    Logger.ErrorFormat("{correlation} Possible DEADLOCK! One of these transactions: {transactions} is in progress for longer then {totalTime}ms",
                        correlation,
                        transactionsInProgress,
                        _settings.GapHandlingSettings.PossibleDeadlockTime);
                }


                if(delayTime > 0)
                {
                    Logger.TraceFormat("{correlation} Delay 'PollTransactions' for {delayTime}ms", correlation, delayTime);
                    await Task.Delay(delayTime, cancellationToken);
                }

                if(count % 5 == 0)
                    delayTime += 10;

                totalTime += sw.ElapsedMilliseconds;
                count++;
                sw.Restart();

                Logger.TraceFormat("{correlation} State 'PollTransactions' | count: {count} | delayTime: {delayTime} | totalTime: {totalTime}", correlation, count, delayTime, totalTime);
            } while(stillInProgress);
        }

        private async Task<(ImmutableArray<StreamMessage>, ImmutableDictionary<string, int>, bool)> ReadTrustedMessages(
            long fromPositionInclusive,
            long toPositionInclusive,
            int maxCount,
            bool prefetch,
            Guid correlation,
            CancellationToken cancellationToken)
        {
            Logger.TraceFormat("{correlation} Read trusted message initiated", correlation);

            var (messages, maxAgeDict, _) = await ReadAllForwards(fromPositionInclusive, maxCount, prefetch, correlation, cancellationToken);

            if(!messages.Any())
            {
                Logger.TraceFormat("{correlation} Read trusted message result is empty.", correlation);
                return (ImmutableArray<StreamMessage>.Empty, ImmutableDictionary<string, int>.Empty, true);
            }

            Logger.TraceFormat("{correlation} Filter messages from {fromPositionInclusive} to {toPositionInclusive}", correlation, fromPositionInclusive, toPositionInclusive);

            var readResultToReturn = messages.Where(x => x.Position >= fromPositionInclusive && x.Position <= toPositionInclusive).ToList();

            var isEnd = IsEnd(messages.Count, maxCount);
            if(isEnd && readResultToReturn.Count <= messages.Count - 1)
                isEnd = false;
            else if(readResultToReturn.Count == maxCount + 1)
                readResultToReturn.RemoveAt(maxCount);

            Logger.TraceFormat("{correlation} IsEnd: {isEnd} | filteredCount: {filteredCount} | totalCount {totalCount}", correlation, isEnd, readResultToReturn.Count, messages.Count);

            return (readResultToReturn.ToImmutableArray(), maxAgeDict.ToImmutableDictionary(), isEnd);
        }

        private async Task<bool> ReadAnyTransactionsInProgress(TxIdList transactionIds, CancellationToken cancellationToken)
        {
            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            using(var command = BuildFunctionCommand(_schema.ReadAnyTransactionsInProgress, transaction, Parameters.Name(connection.Database), Parameters.TransactionIds(transactionIds)))
            {
                var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as bool?;

                return result ?? false;
            }
        }

        private async Task<(List<StreamMessage> messages, Dictionary<string, int> maxAgeDict, TxIdList transactionIdsInProgress)> ReadAllForwards(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            Guid correlation,
            CancellationToken cancellationToken)
        {
            var sw = Stopwatch.StartNew();

            var refcursorSql = new StringBuilder();

            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            {
                using(var command = BuildFunctionCommand(_schema.ReadAll,
                          transaction,
                          Parameters.Count(maxCount + 1),
                          Parameters.Position(fromPositionExclusive),
                          Parameters.ReadDirection(ReadDirection.Forward),
                          Parameters.Prefetch(prefetch)))
                using(var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false))
                {
                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        refcursorSql.AppendLine(Schema.FetchAll(reader.GetString(0)));
                    }
                }

                using(var command = new NpgsqlCommand(refcursorSql.ToString(), transaction.Connection, transaction))
                using(var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false))
                {
                    if(!reader.HasRows)
                    {
                        return (new List<StreamMessage>(), new Dictionary<string, int>(), null);
                    }

                    var messages = new List<StreamMessage>();
                    var maxAgeDict = new Dictionary<string, int>();

                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        if(messages.Count == maxCount)
                        {
                            messages.Add(default);
                        }
                        else
                        {
                            var streamIdInfo = new StreamIdInfo(reader.GetString(0));
                            var (message, maxAge, _) = await ReadAllStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch);

                            if(maxAge.HasValue)
                            {
                                if(!maxAgeDict.ContainsKey(message.StreamId))
                                {
                                    maxAgeDict.Add(message.StreamId, maxAge.Value);
                                }
                            }

                            messages.Add(message);
                        }
                    }

                    var transactionIdsInProgress = new TxIdList();
                    await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        transactionIdsInProgress.Add(await reader.GetFieldValueAsync<long>(0, cancellationToken));
                    }

                    Logger.InfoFormat("{correlation} Query 'ReadAllForwards' took: {timeTaken}ms", correlation, sw.ElapsedMilliseconds);

                    return (messages, maxAgeDict, transactionIdsInProgress);
                }
            }
        }

        protected override async Task<ReadAllPage> ReadAllBackwardsInternal(long fromPositionExclusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            var ordinal = fromPositionExclusive == Position.End ? long.MaxValue : fromPositionExclusive;

            var refcursorSql = new StringBuilder();

            using(var connection = await OpenConnection(cancellationToken))
            using(var transaction = connection.BeginTransaction())
            {
                using(var command = BuildFunctionCommand(_schema.ReadAll,
                          transaction,
                          Parameters.Count(maxCount + 1),
                          Parameters.Position(ordinal),
                          Parameters.ReadDirection(ReadDirection.Backward),
                          Parameters.Prefetch(prefetch)))
                using(var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false))
                {
                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        refcursorSql.AppendLine(Schema.FetchAll(reader.GetString(0)));
                    }
                }


                using(var command = new NpgsqlCommand(refcursorSql.ToString(), transaction.Connection, transaction))
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
                        var (message, maxAge, position) = await ReadAllStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch);

                        if(maxAge.HasValue)
                        {
                            if(!maxAgeDict.ContainsKey(message.StreamId))
                            {
                                maxAgeDict.Add(message.StreamId, maxAge.Value);
                            }
                        }

                        messages.Add(message);

                        lastOrdinal = position;
                    }

                    bool isEnd = true;
                    var nextPosition = lastOrdinal;

                    if(messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                    {
                        isEnd = false;
                        messages.RemoveAt(maxCount);
                    }

                    var filteredMessages = FilterExpired(messages, maxAgeDict);

                    fromPositionExclusive = filteredMessages.Count > 0 ? filteredMessages[0].Position : 0;

                    return new ReadAllPage(fromPositionExclusive, nextPosition, isEnd, ReadDirection.Backward, readNext, filteredMessages.ToArray());
                }
            }
        }

        private async Task<(StreamMessage message, int? maxAge, long position)> ReadAllStreamMessage(DbDataReader reader, PostgresqlStreamId streamId, bool prefetch)
        {
            async Task<string> ReadString(int ordinal)
            {
                if(reader.IsDBNull(ordinal))
                {
                    return null;
                }

                using(var textReader = reader.GetTextReader(ordinal))
                {
                    return await textReader.ReadToEndAsync().ConfigureAwait(false);
                }
            }

            var messageId = reader.GetGuid(1);
            var streamVersion = reader.GetInt32(2);
            var position = reader.GetInt64(3);
            var createdUtc = reader.GetDateTime(4);
            var type = reader.GetString(5);
            var jsonMetadata = await ReadString(6);

            if(prefetch)
            {
                return (new StreamMessage(streamId.IdOriginal, messageId, streamVersion, position, createdUtc, type, jsonMetadata, await ReadString(7)), reader.GetFieldValue<int?>(8), position);
            }

            return (new StreamMessage(streamId.IdOriginal, messageId, streamVersion, position, createdUtc, type, jsonMetadata, ct => GetJsonData(streamId, streamVersion)(ct)),
                reader.GetFieldValue<int?>(8), position);
        }
    }
}