namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Data;
    using System.Data.Common;
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

            var (messages, maxAgeDict, xMin, isEnd) = await ReadAllForwards(fromPositionInclusive, maxCount, prefetch, correlation, cancellationToken).ConfigureAwait(false);

            if(_settings.GapHandlingSettings != null)
            {
                var r = await HandleGaps(messages, maxAgeDict, xMin, isEnd, fromPositionInclusive, maxCount, prefetch, correlation, cancellationToken).ConfigureAwait(false);

                isEnd = r.isEnd;
                messages = r.messages;

                // TODO Change to Tracing before 'REAL RELEASE'
                if(Logger.IsWarnEnabled() && messages.Any())
                {
                    // Check for gap between last page and this. 
                    if(messages[0].Position != fromPositionInclusive)
                    {
                        Logger.WarnFormat("Correlation: {correlation} | Real gap detected on {fromPosition}", correlation, fromPositionInclusive);
                    }

                    for(int i = 0; i < messages.Count - 1; i++)
                    {
                        var expectedNextPosition = messages[i].Position + 1;
                        var actualPosition = messages[i + 1].Position;

                        if(expectedNextPosition != actualPosition)
                        {
                            Logger.WarnFormat("Correlation: {correlation} | Real gap detected on {actualPosition}", correlation, actualPosition);
                        }
                    }
                }
            }

            if(!messages.Any())
            {
                return new ReadAllPage(fromPositionInclusive, fromPositionInclusive, isEnd, ReadDirection.Forward, readNext, Array.Empty<StreamMessage>());
            }

            var filteredMessages = FilterExpired(messages, maxAgeDict);
            var nextPosition = filteredMessages[filteredMessages.Count - 1].Position + 1;

            return new ReadAllPage(fromPositionInclusive, nextPosition, isEnd, ReadDirection.Forward, readNext, filteredMessages.ToArray());
        }

        private async Task<(ReadOnlyCollection<StreamMessage> messages, ReadOnlyDictionary<string, int> maxAgeDict, bool isEnd)> HandleGaps(
            ReadOnlyCollection<StreamMessage> messages,
            ReadOnlyDictionary<string, int> maxAgeDict,
            ulong xMin,
            bool isEnd,
            long fromPositionInclusive,
            int maxCount,
            bool prefetch,
            Guid correlation,
            CancellationToken cancellationToken)
        {
            var hasMessages = messages.Count > 0;

            // We do this as otherwise the Select always enumerates even when trace log is disabled.
            // When we retrieve high amount of messages this will impact the performance.
            if(Logger.IsTraceEnabled())
            {
                Logger.TraceFormat("Correlation: {correlation} | {messages} | Xmin: {xMin}",
                    correlation,
                    hasMessages ? $"Count: {messages.Count} | {string.Join(" | ", messages.Select((x, i) => $"Position: {x.Position}, Array index: {i}, Transaction id: {x.TransactionId}"))}" : "No messages",
                    xMin);
            }

            if(!hasMessages)
            {
                Logger.TraceFormat("Correlation: {correlation} | No messages found. We will return empty list of messages with isEnd to true", correlation, messages);
                return (new ReadOnlyCollection<StreamMessage>(new List<StreamMessage>()), new ReadOnlyDictionary<string, int>(new Dictionary<string, int>()), true);
            }

            var maxTransactionId = messages.Select(x => x.TransactionId).Max();
            if(maxTransactionId < xMin)
            {
                Logger.TraceFormat("Correlation: {correlation} | All messages have a transaction id lower than xMin {xMin}, no need for gap checking", correlation, xMin);
                return (messages, maxAgeDict, isEnd);
            }

            Logger.TraceFormat("Correlation: {correlation} | Danger zone! We have messages and xMin ({xMin}) is not higher than maxTransactionId ({maxTransactionId}), we need to start gap checking",
                correlation,
                xMin,
                maxTransactionId);

            // Check for gap between last page and this. 
            if(messages[0].Position != fromPositionInclusive)
            {
                Logger.TraceFormat(
                    "Correlation: {correlation} | fromPositionInclusive {fromPositionInclusive} does not match first position of received messages {position}",
                    correlation,
                    fromPositionInclusive,
                    messages[0].Position);

                await PollXmin(maxTransactionId, correlation, cancellationToken).ConfigureAwait(false);
                return await ReadTrustedMessages(fromPositionInclusive, messages[messages.Count - 1].Position, maxCount, prefetch, correlation, cancellationToken).ConfigureAwait(false);
            }

            for(int i = 0; i < messages.Count - 1; i++)
            {
                var expectedNextPosition = messages[i].Position + 1;
                var actualPosition = messages[i + 1].Position;
                Logger.TraceFormat("Correlation: {correlation} | Gap checking. Expected position: {expectedNextPosition} | Actual position: {actualPosition}",
                    correlation,
                    expectedNextPosition,
                    actualPosition);

                if(expectedNextPosition != actualPosition)
                {
                    Logger.TraceFormat("Correlation: {correlation} | Gap detected", correlation);

                    await PollXmin(maxTransactionId, correlation, cancellationToken).ConfigureAwait(false);
                    return await ReadTrustedMessages(fromPositionInclusive, messages[messages.Count - 1].Position, maxCount, prefetch, correlation, cancellationToken).ConfigureAwait(false);
                }
            }

            return (messages, maxAgeDict, isEnd);
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

        private async Task<(ReadOnlyCollection<StreamMessage>, ReadOnlyDictionary<string, int>, bool)> ReadTrustedMessages(
            long fromPositionInclusive,
            long toPositionInclusive,
            int maxCount,
            bool prefetch,
            Guid correlation,
            CancellationToken cancellationToken)
        {
            Logger.TraceFormat("Correlation: {correlation} | Read trusted message initiated", correlation);
            var (messages, maxAgeDict, _, isEnd) = await ReadAllForwards(fromPositionInclusive, maxCount, prefetch, correlation, cancellationToken).ConfigureAwait(false);

            Logger.TraceFormat("Correlation: {correlation} | Filter messages from {fromPositionInclusive} to {toPositionInclusive}", correlation, fromPositionInclusive, toPositionInclusive);
            var messageToReturn = messages.Where(x => x.Position >= fromPositionInclusive && x.Position <= toPositionInclusive).ToList();

            if(isEnd && messageToReturn.Count != messages.Count)
                isEnd = false;

            Logger.TraceFormat("Correlation: {correlation} | IsEnd: {isEnd} | FilteredCount: {filteredCount} | TotalCount: {totalCount}", correlation, isEnd, messageToReturn.Count, messages.Count);

            return (messageToReturn.AsReadOnly(), new ReadOnlyDictionary<string, int>(maxAgeDict), isEnd);
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

        private async Task<(ReadOnlyCollection<StreamMessage> messages, ReadOnlyDictionary<string, int> maxAgeDict, ulong xMin, bool isEnd)> ReadAllForwards(
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
                        
                        return (new List<StreamMessage>().AsReadOnly(), new ReadOnlyDictionary<string, int>(new Dictionary<string, int>()), xMin, true);
                    }

                    var messages = new List<StreamMessage>();
                    var maxAgeDict = new Dictionary<string, int>();
                    var isEnd = true;


                    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        if(messages.Count == maxCount)
                            isEnd = false;
                        else
                        {
                            var streamIdInfo = new StreamIdInfo(reader.GetString(0));
                            var (message, maxAge, _) = await ReadAllStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch).ConfigureAwait(false);

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
                    
                    return (messages.AsReadOnly(), new ReadOnlyDictionary<string, int>(maxAgeDict), xMin, isEnd);
                }
            }
        }

        protected override async Task<ReadAllPage> ReadAllBackwardsInternal(long fromPositionExclusive, int maxCount, bool prefetch, ReadNextAllPage readNext, CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            var ordinal = fromPositionExclusive == Position.End ? long.MaxValue : fromPositionExclusive;

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
                        var (message, maxAge, position) = await ReadAllStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch).ConfigureAwait(false);

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

                    var filteredMessages = FilterExpired(messages.AsReadOnly(), new ReadOnlyDictionary<string, int>(maxAgeDict));

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
            var transactionId = reader.GetFieldValue<ulong>(6);
            var jsonMetadata = await ReadString(7).ConfigureAwait(false);

            if(prefetch)
            {
                return (new StreamMessage(streamId.IdOriginal, messageId, streamVersion, position, createdUtc, type, jsonMetadata, await ReadString(8).ConfigureAwait(false), transactionId),
                    reader.GetFieldValue<int?>(9), position);
            }

            return (new StreamMessage(streamId.IdOriginal, messageId, streamVersion, position, createdUtc, type, jsonMetadata, ct => GetJsonData(streamId, streamVersion)(ct), transactionId),
                reader.GetFieldValue<int?>(8), position);
        }
    }
}