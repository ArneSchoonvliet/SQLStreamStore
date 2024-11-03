namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Data;
    using System.Diagnostics;
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
        protected override async Task<ReadAllPage> ReadAllForwardsInternal(long fromPositionInclusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            var correlation = Guid.NewGuid();

            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;

            var page = await ReadAllForwards(fromPositionInclusive, maxCount, prefetch, correlation, cancellationToken).ConfigureAwait(false);

            if(_settings.GapHandlingSettings != null)
            {
                var (messages, _, isEnd) = await HandleGaps(page, fromPositionInclusive, maxCount, prefetch, correlation, cancellationToken).ConfigureAwait(false);

                if(Logger.IsTraceEnabled() && messages.Count != 0)
                {
                    var expectedStartPosition = fromPositionInclusive;
                    var actualStartPosition = messages[0].Position;

                    // Check for gap between last page and this. 
                    if(expectedStartPosition != actualStartPosition)
                    {
                        Logger.TraceFormat("Correlation: {correlation} | Real gap detected on {position}", correlation, expectedStartPosition);
                    }

                    for(int i = 0; i < messages.Count - 1; i++)
                    {
                        var expectedNextPosition = messages[i].Position + 1;
                        var actualNextPosition = messages[i + 1].Position;

                        if(expectedNextPosition != actualNextPosition)
                        {
                            Logger.TraceFormat("Correlation: {correlation} | Real gap detected on {position}", correlation, expectedNextPosition);
                        }
                    }
                }
            }

            if(page.Messages.Count == 0)
            {
                return new ReadAllPage(fromPositionInclusive, fromPositionInclusive, page.IsEnd, ReadDirection.Forward, readNext, Array.Empty<StreamMessage>());
            }

            var filteredMessages = FilterExpired(page.Messages, page.MaxAgeDict);
            var nextPosition = filteredMessages[^1].Position + 1;

            return new ReadAllPage(fromPositionInclusive, nextPosition, page.IsEnd, ReadDirection.Forward, readNext, filteredMessages.ToArray());
        }

        private async Task<(ReadOnlyCollection<StreamMessage> messages, ReadOnlyDictionary<string, int> maxAgeDict, bool isEnd)> HandleGaps(
            ReadForwardsPage page,
            long fromPositionInclusive,
            int maxCount,
            bool prefetch,
            Guid correlation,
            CancellationToken cancellationToken)
        {
            var (messages, maxAgeDict, transactionIdDict, xMin, isEnd) = page;
            var hasMessages = messages.Count > 0;

            // Avoid unnecessary enumeration in logs when trace logging is disabled.
            if(Logger.IsTraceEnabled())
            {
                Logger.TraceFormat("Correlation: {correlation} | {messages} | Xmin: {xMin}",
                    correlation,
                    hasMessages
                        ? $"Count: {messages.Count} | {string.Join(" | ", messages.Select((x, i) => $"Position: {x.Position}, Array index: {i}, Transaction id: {transactionIdDict[x.Position]}"))}"
                        : "No messages",
                    xMin);
            }

            if(!hasMessages)
            {
                Logger.TraceFormat("Correlation: {correlation} | No messages found. We will return empty list of messages with isEnd to true", correlation, messages);
                return (new ReadOnlyCollection<StreamMessage>(new List<StreamMessage>()), new ReadOnlyDictionary<string, int>(new Dictionary<string, int>()), true);
            }
            
            /*
             * Problem Summary:
             * This event store system, backed by a PostgreSQL database, assigns positions to events using a sequence.
             * Due to high levels of parallel transactions, we encounter "gaps" in the sequence for several reasons:
             *
             * 1. In-flight Transactions:
             *    - When multiple transactions are running concurrently, some transactions may receive a lower position
             *      but complete later, creating temporary "gaps" in the sequence.
             *    - To detect whether a gap is temporary or permanent, we use PostgreSQL’s `pg_current_snapshot()`
             *      function to retrieve xmin, which marks the minimum transaction ID visible to the current transaction.
             *    - Our logic polls the database, re-reading messages until xmin surpasses the maxTransactionId
             *      observed in the current read batch. This ensures that any "in-flight" transactions with a lower
             *      position but delayed visibility will have become visible before we determine if the gap is permanent.
             *
             *    - Theoretical Sufficiency:
             *      - In theory, this polling approach should be enough to distinguish between temporary and permanent gaps,
             *        allowing us to avoid skipping valid events due to transactions that are still in-flight but not yet visible.
             *      - Once xmin is higher than maxTransactionId, we can safely assume that any gaps are permanent,
             *        typically due to rollbacks.
             *
             * 2. Handling Unexpected Anomalies:
             *    - In practice, we occasionally observe cases where a row with a lower position has a higher transaction ID,
             *      which can disrupt this algorithm by causing false gaps that do not resolve even after repeated polling.
             *      See: https://dba.stackexchange.com/questions/342896/in-what-case-can-a-new-transaction-claim-an-older-sequence-id-in-postgresql
             *    - To address this, we introduce a SafetyTransactionGap setting, which defines a buffer zone above
             *      maxTransactionId within which we consider transactions as potentially still in-flight.
             *
             * 3. Safety Gap Mechanism:
             *    - If the difference between xmin and maxTransactionId is smaller than the configured SafetyTransactionGap,
             *      we assume the gap could still be due to an in-flight transaction and will wait briefly before re-reading messages.
             *    - This delay time is configurable using DelayTime, which sets the interval to wait before polling again,
             *      helping to ensure all in-flight transactions have a chance to become visible.
             *    - If the gap between xmin and maxTransactionId exceeds the SafetyTransactionGap, we treat these as real gaps
             *      (likely due to rollbacks) and proceed without further delay, as the missing messages are assumed to be permanent.
             */
            
            // Determine the highest transaction ID from the current messages.
            // This is the minimum value we want xMin to exceed to be in a "safe" state.
            var maxTransactionId = transactionIdDict.Select(x => x.Value).Max();
            var gapHandlingBehaviour = DetermineGapHandlingBehaviour(xMin, maxTransactionId, correlation);
            
            // If we don't need to handle the gaps, return immediately. It should now contain no gaps are permanent gaps
            if (gapHandlingBehaviour == GapHandelingBehaviour.None)
                return (messages, maxAgeDict, isEnd);
            
            // Check for any gaps between received messages and fromPositionInclusive or within the current page.
            // If this isn't the case we are sure there are no gaps and we are safe to return the messages
            if(!HasGapWithPreviousPage(messages, fromPositionInclusive, correlation) && !HasGapsWithinPage(messages, correlation))
                return (messages, maxAgeDict, isEnd);
            
            Logger.TraceFormat("Correlation: {correlation} | Detected gap, initiating handling mechanism {gapHandlingBehaviour}", correlation, gapHandlingBehaviour);
                
            switch(gapHandlingBehaviour)
            {
                case GapHandelingBehaviour.PollXmin:
                    await PollXmin(maxTransactionId, correlation, cancellationToken);
                    break;
                case GapHandelingBehaviour.StaticDelay:
                    await Delay(_settings.GapHandlingSettings.DelayTime, correlation, cancellationToken);
                    break;
                default:
                    throw new NotImplementedException($"Unknown gap handling behaviour: {gapHandlingBehaviour}");
            }
            
            var stablePage = await ReadBetween(
                fromPositionInclusive, 
                messages[^1].Position, 
                maxCount, 
                prefetch, 
                correlation, 
                cancellationToken
            ).ConfigureAwait(false);
                
            // Polling theoretically ensures gaps have stabilized, with `xMin` exceeding the latest transaction ID.
            // However, given rare edge cases, we still verify that `xMin` is safely distanced from `maxTransactionId`.
            if (gapHandlingBehaviour == GapHandelingBehaviour.PollXmin) 
                return await HandleGaps(stablePage, fromPositionInclusive, maxCount, prefetch, correlation, cancellationToken)
                    .ConfigureAwait(false);
                
            return (stablePage.Messages, stablePage.MaxAgeDict, stablePage.IsEnd);
        }
        
        private GapHandelingBehaviour DetermineGapHandlingBehaviour(ulong xMin, ulong maxTransactionId, Guid correlation)
        {
            // If xMin is greater than maxTransactionId, theoretically we should be safe.
            if(xMin > maxTransactionId)
            {
                // No SafetyTransactionGap configured, so gaps are assumed to be real, and messages can be safely returned.
                if(!_settings.GapHandlingSettings.SafetyTransactionGap.HasValue)
                {
                    Logger.TraceFormat("Correlation: {correlation} | All messages have a transaction ID lower than xMin ({xMin}), no need for gap checking.", correlation, xMin);
                    return GapHandelingBehaviour.None;
                }

                // SafetyTransactionGap is configured. If xMin exceeds maxTransactionId + safety gap, gaps are assumed to be permanent, and messages can be returned.
                if(xMin > maxTransactionId + _settings.GapHandlingSettings.SafetyTransactionGap.Value)
                {
                    Logger.TraceFormat("Correlation: {correlation} | All messages have a transaction ID lower than xMin ({xMin}) + safety gap ({safetyGap}), no need for gap checking.",
                        correlation,
                        xMin,
                        _settings.GapHandlingSettings.SafetyTransactionGap);
                    return GapHandelingBehaviour.None;
                }

                // xMin exceeds maxTransactionId but falls within the safety gap threshold, so we set a static delay (DelayTime) before re-reading to resolve in-flight transactions.
                Logger.InfoFormat(
                    "Correlation: {correlation} | Caution! Detected a too narrow gap between maxTransactionId ({maxTransactionId}) and xMin ({xMin}), within safety threshold ({safetyGap}). Engaging static delay.",
                    correlation,
                    maxTransactionId,
                    xMin,
                    _settings.GapHandlingSettings.SafetyTransactionGap,
                    _settings.GapHandlingSettings.DelayTime ?? 0);

                // Set a static delay, as polling xMin isn’t beneficial since xMin already exceeds maxTransactionId.
                return GapHandelingBehaviour.StaticDelay;
            }
            
            // xMin is not greater than maxTransactionId, so initiate gap checking by polling xMin.
            Logger.InfoFormat(
                "Correlation: {correlation} | Caution! xMin ({xMin}) is not higher than maxTransactionId ({maxTransactionId}), engaging polling for gap checking.",
                correlation,
                xMin,
                maxTransactionId);

            return GapHandelingBehaviour.PollXmin;
        }

        // Checks if there’s a gap between the provided starting position and the first message's position in the current page.
        private bool HasGapWithPreviousPage(ReadOnlyCollection<StreamMessage> messages, long fromPositionInclusive, Guid correlation)
        {
            var hasGap = messages[0].Position != fromPositionInclusive;
            if (hasGap)
            {
                Logger.TraceFormat(
                    "Correlation: {correlation} | fromPositionInclusive {fromPositionInclusive} does not match first position of received messages {position}",
                    correlation,
                    fromPositionInclusive,
                    messages[0].Position);
            }
            return hasGap;
        }

        // Checks for gaps within the positions in a single page of messages.
        private bool HasGapsWithinPage(ReadOnlyCollection<StreamMessage> messages, Guid correlation)
        {
            for (int i = 0; i < messages.Count - 1; i++)
            {
                var expectedNextPosition = messages[i].Position + 1;
                var actualPosition = messages[i + 1].Position;
        
                Logger.TraceFormat(
                    "Correlation: {correlation} | Gap checking. Expected position: {expectedNextPosition} | Actual position: {actualPosition}",
                    correlation,
                    expectedNextPosition,
                    actualPosition);

                if (expectedNextPosition != actualPosition)
                {
                    Logger.TraceFormat("Correlation: {correlation} | Gap detected", correlation);
                    return true;
                }
            }
            return false;
        }
        
        private async Task Delay(int? delay, Guid correlation, CancellationToken cancellationToken)
        {
            var nonNullableDelay = delay ?? 0;
            Logger.TraceFormat("Correlation: {correlation} | Gaps detected but xMin was already reached, waiting for {delay}ms", correlation, delay);
            await Task.Delay(nonNullableDelay, cancellationToken).ConfigureAwait(false);
        }

        private async Task PollXmin(ulong maximumTransactionId, Guid correlation, CancellationToken cancellationToken)
        {
            Logger.TraceFormat("Correlation: {correlation} | Gaps might be filled, start polling 'Xmin', needs to reach at least {transactionId}", correlation, maximumTransactionId);

            var count = 0;
            var delayTime = 0;
            var totalTime = 0L;

            var sw = new Stopwatch();

            while(true)
            {
                sw.Restart();

                if(delayTime > 0)
                {
                    Logger.TraceFormat("Correlation: {correlation} | Delay 'PollXmin' for {delayTime}ms", correlation, delayTime);
                    await Task.Delay(delayTime, cancellationToken).ConfigureAwait(false);

                    totalTime += sw.ElapsedMilliseconds;
                    sw.Restart();
                }

                if(count % 5 == 0)
                    delayTime += 10;

                var xMin = await ReadXmin(cancellationToken).ConfigureAwait(false);
                var stillInProgress = maximumTransactionId >= xMin;

                totalTime += sw.ElapsedMilliseconds;

                // All should be fine, if we now re-query the same page it should only contain 'real gaps'
                if(!stillInProgress)
                {
                    Logger.TraceFormat(
                        "Correlation: {correlation} | Transactions not pending anymore | Total Polling time: {totalTime}ms, xMin: {xMin}, maximumTransactionId: {transactionId}",
                        correlation,
                        totalTime,
                        xMin,
                        maximumTransactionId);
                    return;
                }

                // We are still in the 'Danger zone', in flight transactions are not done yet. BUT we have reached our limit time we wait for it...
                // In this case we are rather sure we have a skipped event.
                if(totalTime >= _settings.GapHandlingSettings.SkipTime)
                {
                    Logger.ErrorFormat(
                        "Correlation: {correlation} | Possible SKIPPED EVENT as we will stop waiting for in progress transactions! One of the transactions is in progress for to long (>= {skipTime}) | Total Polling time: {totalTime}ms, xMin: {xMin}, maximumTransactionId: {transactionId}",
                        correlation,
                        _settings.GapHandlingSettings.SkipTime,
                        totalTime,
                        xMin,
                        maximumTransactionId);
                    return;
                }

                // We are still in the 'Danger zone', in flight transactions are not done yet. We still have a chance on a 'skipped event'
                Logger.TraceFormat(
                    "Correlation: {correlation} | Transactions still pending (Iteration: {count}). Query 'ReadXmin' took: {timeTaken}ms | Total Polling time: {totalTime}ms, xMin: {xMin}, maximumTransactionId: {transactionId}",
                    correlation,
                    count + 1,
                    sw.ElapsedMilliseconds,
                    totalTime,
                    xMin,
                    maximumTransactionId);

                // Log some warnings since it's starting to take a while...
                if(totalTime >= _settings.GapHandlingSettings.MinimumWarnTime)
                {
                    Logger.WarnFormat(
                        "Correlation: {correlation} | Possible DEADLOCK! One of the transactions is taking some time (>= {warnTime}) | Total Polling time: {totalTime}ms, xMin: {xMin}, maximumTransactionId: {transactionId}",
                        correlation,
                        _settings.GapHandlingSettings.MinimumWarnTime,
                        totalTime,
                        xMin,
                        maximumTransactionId);
                }

                count++;
            }
        }

        private async Task<ReadForwardsPage> ReadBetween(
            long fromPositionInclusive,
            long toPositionInclusive,
            int maxCount,
            bool prefetch,
            Guid correlation,
            CancellationToken cancellationToken)
        {
            Logger.TraceFormat("Correlation: {correlation} | Read trusted message initiated", correlation);
            var (messages, maxAgeDict, transactionIdDict, xMin, isEnd) = await ReadAllForwards(fromPositionInclusive, maxCount, prefetch, correlation, cancellationToken).ConfigureAwait(false);

            Logger.TraceFormat("Correlation: {correlation} | Filter messages from {fromPositionInclusive} to {toPositionInclusive}", correlation, fromPositionInclusive, toPositionInclusive);
            var messageToReturn = messages.Where(x => x.Position >= fromPositionInclusive && x.Position <= toPositionInclusive).ToList();

            if(isEnd && messageToReturn.Count != messages.Count)
                isEnd = false;

            Logger.TraceFormat("Correlation: {correlation} | IsEnd: {isEnd} | FilteredCount: {filteredCount} | TotalCount: {totalCount}", correlation, isEnd, messageToReturn.Count, messages.Count);
            return new ReadForwardsPage(messageToReturn.AsReadOnly(), new ReadOnlyDictionary<string, int>(maxAgeDict), transactionIdDict, xMin, isEnd);
        }

        private async Task<ulong> ReadXmin(CancellationToken cancellationToken)
        {
            using(var connection = await OpenConnection(cancellationToken).ConfigureAwait(false))
            using(var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            using(var command = BuildFunctionCommand(_schema.ReadXmin, transaction))
            {
                var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as ulong?;
                return result ?? 0;
            }
        }

        private async Task<ReadForwardsPage> ReadAllForwards(
            long fromPositionInclusive,
            int maxCount,
            bool prefetch,
            Guid correlation,
            CancellationToken cancellationToken)
        {
            var sw = Stopwatch.StartNew();

            var refcursorSql = new StringBuilder();

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
                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        refcursorSql.AppendLine(Schema.FetchAll(reader.GetString(0)));
                    }
                }

                using(var command = new NpgsqlCommand(refcursorSql.ToString(), transaction.Connection, transaction))
                using(var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken).ConfigureAwait(false))
                {
                    await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                    var xMin = await reader.GetFieldValueAsync<ulong>(0, cancellationToken).ConfigureAwait(false);
                    await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);

                    if(!reader.HasRows)
                    {
                        Logger.TraceFormat(
                            "Correlation: {correlation} | Query 'ReadAllForwards' took: {timeTaken}ms | fromPositionInclusive: {fromPositionInclusive}, maxCount: {maxCount}, prefetch: {preFetch} | count: {messageCount}, xMin: {xMin}, isEnd: {isEnd}",
                            correlation,
                            sw.ElapsedMilliseconds,
                            fromPositionInclusive,
                            maxCount,
                            prefetch,
                            0,
                            xMin,
                            true);

                        return new ReadForwardsPage(new List<StreamMessage>().AsReadOnly(),
                            new ReadOnlyDictionary<string, int>(new Dictionary<string, int>()),
                            new ReadOnlyDictionary<long, ulong>(new Dictionary<long, ulong>()),
                            xMin,
                            true);
                    }

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
                        "Correlation: {correlation} | Query 'ReadAllForwards' took: {timeTaken}ms | fromPositionInclusive: {fromPositionInclusive}, maxCount: {maxCount}, prefetch: {preFetch} | count: {messageCount}, xMin: {xMin}, isEnd: {isEnd}",
                        correlation,
                        sw.ElapsedMilliseconds,
                        fromPositionInclusive,
                        maxCount,
                        prefetch,
                        messages.Count,
                        xMin,
                        isEnd);

                    return new ReadForwardsPage(
                        messages.AsReadOnly(),
                        new ReadOnlyDictionary<string, int>(maxAgeDict),
                        new ReadOnlyDictionary<long, ulong>(transactionIdDict),
                        xMin,
                        isEnd);
                }
            }
        }

        protected override async Task<ReadAllPage> ReadAllBackwardsInternal(long fromPositionInclusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            var ordinal = fromPositionInclusive == Position.End ? long.MaxValue : fromPositionInclusive;

            var refcursorSql = new StringBuilder();

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
                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        var result = reader.GetString(0);

                        // ReadBackwards doesn't need to retrieve tx_info since currently not supported to check for gaps
                        // TODO Maybe consider adding it since gaps can also be a problem in this case
                        if(result == "tx_info")
                            continue;

                        refcursorSql.AppendLine(Schema.FetchAll(result));
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

        private async Task<(StreamMessage message, int? maxAge, ulong transactionId)> ReadAllStreamMessage(NpgsqlDataReader reader, PostgresqlStreamId streamId, bool prefetch)
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

        private enum GapHandelingBehaviour
        {
            None = 1,
            PollXmin = 2,
            StaticDelay = 3
        }
        
        private class ReadForwardsPage
        {
            public ReadOnlyCollection<StreamMessage> Messages { get; }
            public ReadOnlyDictionary<string, int> MaxAgeDict { get; }
            public ReadOnlyDictionary<long, ulong> TransactionIdDict { get; }
            public ulong XMin { get; }
            public bool IsEnd { get; }

            public ReadForwardsPage(ReadOnlyCollection<StreamMessage> messages, ReadOnlyDictionary<string, int> maxAgeDict, ReadOnlyDictionary<long, ulong> transactionIdDict, ulong xMin, bool isEnd)
            {
                Messages = messages;
                MaxAgeDict = maxAgeDict;
                TransactionIdDict = transactionIdDict;
                XMin = xMin;
                IsEnd = isEnd;
            }

            public void Deconstruct(
                out ReadOnlyCollection<StreamMessage> messages,
                out ReadOnlyDictionary<string, int> maxAgeDict,
                out ReadOnlyDictionary<long, ulong> transactionIdDict,
                out ulong xMin,
                out bool isEnd)
            {
                messages = Messages;
                maxAgeDict = MaxAgeDict;
                transactionIdDict = TransactionIdDict;
                xMin = XMin;
                isEnd = IsEnd;
            }
        }
    }
}