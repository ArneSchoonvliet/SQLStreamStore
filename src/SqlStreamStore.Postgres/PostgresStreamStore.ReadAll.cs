namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
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
        protected override async Task<ReadAllPage> ReadAllForwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            var correlation = Guid.NewGuid();

            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;

            var (readAllResult, transactionIdsInProgress) = await ReadAllForwards(fromPositionExclusive, null, maxCount, prefetch, correlation, cancellationToken);

            var gapHandleResult = await HandleGaps(readAllResult,
                transactionIdsInProgress,
                fromPositionExclusive,
                maxCount,
                prefetch,
                correlation,
                cancellationToken);
            
            readAllResult = gapHandleResult.ReadAllResult;

            if (!readAllResult.Any())
            {
                return new ReadAllPage(
                    fromPositionExclusive,
                    fromPositionExclusive,
                    gapHandleResult.isEnd,
                    ReadDirection.Forward,
                    readNext,
                    Array.Empty<StreamMessage>());
            }

            var filteredMessages = FilterExpired(readAllResult);

            var nextPosition = filteredMessages[filteredMessages.Count - 1].Position + 1;

            return new ReadAllPage(
                fromPositionExclusive,
                nextPosition,
                gapHandleResult.isEnd,
                ReadDirection.Forward,
                readNext,
                filteredMessages.ToArray());
        }

        private async Task<(List<(StreamMessage message, int? maxAge)> ReadAllResult, TxIdList TransactionIdsInProgress)> ReadAllForwards(
            long fromPositionExclusive,
            long? toPositionInclusive,
            int maxCount,
            bool prefetch,
            Guid correlation,
            CancellationToken cancellationToken)
        {
            var sw = Stopwatch.StartNew();

            var refcursorSql = new StringBuilder();

            using (var connection = await OpenConnection(cancellationToken))
            using (var transaction = connection.BeginTransaction())
            {
                using (var command = BuildFunctionCommand(
                _schema.ReadAll,
                transaction,
                Parameters.Count(maxCount + 1),
                Parameters.Position(fromPositionExclusive),
                Parameters.OptionalPosition(toPositionInclusive),
                Parameters.ReadDirection(ReadDirection.Forward),
                Parameters.Prefetch(prefetch)))
                using (var reader = await command
                    .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                    .ConfigureAwait(false))
                {
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        refcursorSql.AppendLine(Schema.FetchAll(reader.GetString(0)));
                    }
                }

                using (var command = new NpgsqlCommand(refcursorSql.ToString(), transaction.Connection, transaction))
                using (var reader = await command
                    .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                    .ConfigureAwait(false))
                {

                    if (!reader.HasRows)
                    {
                        return (new List<(StreamMessage message, int? maxAge)>(), new TxIdList());
                    }

                    var messages = new List<(StreamMessage message, int? maxAge)>();

                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        if (messages.Count == maxCount)
                        {
                            messages.Add(default);
                        }
                        else
                        {
                            var streamIdInfo = new StreamIdInfo(reader.GetString(0));
                            var (message, maxAge, _) =
                                await ReadAllStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch);
                            messages.Add((message, maxAge));
                        }
                    }

                    var transactionIdsInProgress = new TxIdList();
                    await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        transactionIdsInProgress.Add(await reader.GetFieldValueAsync<long>(0, cancellationToken));
                    }

                    Logger.InfoFormat("{correlation} Query 'ReadAllForwards' took: {timeTaken}ms", correlation, sw.ElapsedMilliseconds);



                    return (messages, transactionIdsInProgress);
                }
            }
        }

        private static bool IsEnd(int messageCount, int maxCount)
        {
            bool isEnd = messageCount != maxCount + 1;

            return isEnd;
        }

        private async Task<(List<(StreamMessage message, int? maxAge)> ReadAllResult, bool isEnd)> HandleGaps(
            List<(StreamMessage message, int? maxAge)> readAllResult,
            TxIdList transactionsInProgress,
            long fromPositionInclusive,
            int maxCount,
            bool prefetch,
            Guid correlation,
            CancellationToken cancellationToken)
        {
            if(_settings.GapHandlingSettings == null)
            {
               var isEnd = IsEnd(readAllResult.Count, maxCount);
                if (!isEnd)
                    readAllResult.RemoveAt(maxCount);

                return (readAllResult, isEnd);
            }

            var messages = readAllResult.Select(x => x.message).ToList();

            // We need to remove the last message if we read past the actual maxCount, otherwise gap checking will not be correct as last message is one with default values
            if (messages.Count == maxCount + 1)
                messages.RemoveAt(maxCount);

            var hasMessages = messages.Any();

            Logger.InfoFormat("{correlation} Messages: {messages}", correlation, hasMessages ? string.Join("|", messages.Select((x, i) => $"Position: {x.Position} Array index: {i}")) : "No messages");
            Logger.InfoFormat("{correlation} Gap checking. Message count: {messageCount} | Transactions in progress: {transactionsInProgress}", correlation, messages.Count, transactionsInProgress);

            // Local function, messages are by reference.
            async Task<(List<(StreamMessage message, int? maxAge)>, bool)> RetrieveMessages()
            {
                var (newReadAllResult, _) = await ReadAllForwards(fromPositionInclusive,
                    null,
                    maxCount,
                    prefetch,
                    correlation,
                    cancellationToken);

                if (!newReadAllResult.Any())
                {
                    Logger.InfoFormat("{correlation} New message result is empty.", correlation);
                    return (new List<(StreamMessage message, int? maxAge)>(), true);
                }

                var toPositionInclusive = messages[messages.Count - 1].Position;

                var isEnd = IsEnd(newReadAllResult.Count, maxCount);

                Logger.InfoFormat("{correlation} Filter messages from {fromPositionInclusive} to {toPositionInclusive}", correlation, fromPositionInclusive, toPositionInclusive);

                var readResultToReturn = newReadAllResult.Where(x =>
                    x.message.Position >= fromPositionInclusive && x.message.Position <= toPositionInclusive).ToList();


                Logger.InfoFormat("{correlation} IsEnd: {isEnd} | filteredCount: {filteredCount} | totalCount {totalCount}", correlation, isEnd, readResultToReturn.Count, newReadAllResult.Count);
                // TODO: Check if this count logic makes any sense
                if (isEnd && readResultToReturn.Count <= newReadAllResult.Count - 1) // An extra row was read, we're not at the end
                {
                    isEnd = false;
                }
                else
                {
                    readResultToReturn.RemoveAt(maxCount);
                }

                return (readResultToReturn, isEnd);
            }

            // When transactions are in progress and no messages we need to start polling. It could by all means be possible that all the messages are still in flight transactions.
            // We will not poll transactions and set isEnd to false;
            // TODO: check if isEnd to false really is a desired behaviour. It might make more sense to have it on true. 
            if (!hasMessages) // TODO: check for transactions???
            {
                Logger.InfoFormat("{correlation} No messages found, but transactions: {transactions} in progress. We will return empty list of messages with isEnd to false", correlation, messages);
                return (new List<(StreamMessage message, int? maxAge)>(), false);
            }

            if (!transactionsInProgress.Any())
            {
                Logger.InfoFormat("{correlation} No transactions in progress, no need for gap checking", correlation);

                var isEnd = IsEnd(readAllResult.Count, maxCount);
                if (!isEnd)
                    readAllResult.RemoveAt(maxCount);

                return (readAllResult, isEnd);
            }

            // Check for gap between last page and this. 
            // TODO: Check if we are not allowing same position twice
            if (messages[0].Position != fromPositionInclusive)
            {
                Logger.InfoFormat("{correlation} fromPositionInclusive {fromPositionInclusive} does not match first position of received messages {position} and transactions: {transactions} in progress", correlation, fromPositionInclusive, transactionsInProgress);
                await PollTransactions(correlation, transactionsInProgress, cancellationToken);
                return await RetrieveMessages();
            }


            for (int i = 0; i < messages.Count - 1; i++)
            {
                var expectedNextPosition = messages[i].Position + 1;
                var actualPosition = messages[i + 1].Position;
                Logger.InfoFormat("{correlation} Gap checking. Expected position: {expectedNextPosition} | Actual position: {actualPosition}", correlation, expectedNextPosition, actualPosition);

                if (expectedNextPosition != actualPosition)
                {
                    Logger.InfoFormat("{correlation} Gap detected", correlation);

                    await PollTransactions(correlation, transactionsInProgress, cancellationToken);

                    return await RetrieveMessages();
                }
            }

            if(IsEnd(readAllResult.Count, maxCount))
                return (readAllResult, true);

            readAllResult.RemoveAt(maxCount);
            return (readAllResult, false);

        }

        private async Task PollTransactions(Guid correlation, TxIdList transactionsInProgress, CancellationToken cancellationToken)
        {
            Logger.InfoFormat("{correlation} Transactions {transactions} are in progress, so gaps might be filled, start comparing", correlation, transactionsInProgress);

            bool stillInProgress;
            int count = 1;
            int delayTime = 0;
            long totalTime = 0;

            // TODO: Make global config;
            var allowSkip = true;
            var possibleDeadlockTime = TimeSpan.FromMinutes(1).TotalMilliseconds;
            var possibleAllowedSkipTime = TimeSpan.FromMinutes(2).TotalMilliseconds;

            var sw = Stopwatch.StartNew();
            do
            {
                stillInProgress = await ReadAnyTransactionsInProgress(transactionsInProgress, cancellationToken);
                Logger.InfoFormat("{correlation} Transactions still pending. Query 'ReadAnyTransactionsInProgress' took: {timeTaken}ms", correlation, sw.ElapsedMilliseconds);

                if (allowSkip && totalTime > possibleAllowedSkipTime)
                {
                    Logger.ErrorFormat("{correlation} Possible SKIPPED EVENT as we will stop waiting for in progress transactions! One of these transactions: {transactions} is in progress for longer then {totalTime}ms", correlation, transactionsInProgress);
                    return;
                }

                if (totalTime > possibleDeadlockTime)
                {
                    Logger.ErrorFormat("{correlation} Possible DEADLOCK! One of these transactions: {transactions} is in progress for longer then {totalTime}ms", correlation, transactionsInProgress, totalTime);
                }


                if (delayTime > 0)
                {
                    Logger.InfoFormat("{correlation} Delay 'PollTransactions' for {delayTime}ms", correlation, delayTime);
                    await Task.Delay(delayTime, cancellationToken);
                }

                if (count % 5 == 0)
                    delayTime += 10;

                totalTime += sw.ElapsedMilliseconds;
                count++;
                sw.Restart();

                Logger.TraceFormat("{correlation} State 'PollTransactions' | count: {count} | delayTime: {delayTime} | totalTime: {totalTime}", correlation, count, delayTime, totalTime);

            } while (stillInProgress);
        }

        private async Task<bool> ReadAnyTransactionsInProgress(TxIdList transactionIds, CancellationToken cancellationToken)
        {
            using (var connection = await OpenConnection(cancellationToken))
            using (var transaction = connection.BeginTransaction())
            using (var command = BuildFunctionCommand(
                      _schema.ReadAnyTransactionsInProgress,
                      transaction,
                      Parameters.Name(connection.Database),
                      Parameters.TransactionIds(transactionIds)))
            {
                var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as bool?;

                return result ?? false;
            }
        }

        protected override async Task<ReadAllPage> ReadAllBackwardsInternal(
            long fromPositionExclusive,
            int maxCount,
            bool prefetch,
            ReadNextAllPage readNext,
            CancellationToken cancellationToken)
        {
            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;
            var ordinal = fromPositionExclusive == Position.End ? long.MaxValue : fromPositionExclusive;

            using (var connection = await OpenConnection(cancellationToken))
            using (var transaction = connection.BeginTransaction())
            using (var command = BuildFunctionCommand(
                _schema.ReadAll,
                transaction,
                Parameters.Count(maxCount + 1),
                Parameters.Position(ordinal),
                Parameters.ReadDirection(ReadDirection.Backward),
                Parameters.Prefetch(prefetch)))
            using (var reader = await command
                .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
                .ConfigureAwait(false))
            {
                if (!reader.HasRows)
                {
                    // When reading backwards and there are no more items, then next position is LongPosition.Start,
                    // regardless of what the fromPosition is.
                    return new ReadAllPage(
                        Position.Start,
                        Position.Start,
                        true,
                        ReadDirection.Backward,
                        readNext,
                        Array.Empty<StreamMessage>());
                }

                var messages = new List<(StreamMessage message, int? maxAge)>();

                long lastOrdinal = 0;
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var streamIdInfo = new StreamIdInfo(reader.GetString(0));
                    var (message, maxAge, position) = await ReadAllStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch);
                    messages.Add((message, maxAge));

                    lastOrdinal = position;
                }

                bool isEnd = true;
                var nextPosition = lastOrdinal;

                if (messages.Count == maxCount + 1) // An extra row was read, we're not at the end
                {
                    isEnd = false;
                    messages.RemoveAt(maxCount);
                }

                var filteredMessages = FilterExpired(messages);

                fromPositionExclusive = filteredMessages.Count > 0 ? filteredMessages[0].Position : 0;

                return new ReadAllPage(
                    fromPositionExclusive,
                    nextPosition,
                    isEnd,
                    ReadDirection.Backward,
                    readNext,
                    filteredMessages.ToArray());
            }
        }

        private async Task<(StreamMessage message, int? maxAge, long position)> ReadAllStreamMessage(
            DbDataReader reader,
            PostgresqlStreamId streamId,
            bool prefetch)
        {
            async Task<string> ReadString(int ordinal)
            {
                if (reader.IsDBNull(ordinal))
                {
                    return null;
                }

                using (var textReader = reader.GetTextReader(ordinal))
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

            if (prefetch)
            {
                return (
                    new StreamMessage(
                        streamId.IdOriginal,
                        messageId,
                        streamVersion,
                        position,
                        createdUtc,
                        type,
                        jsonMetadata,
                        await ReadString(7)),
                    reader.GetFieldValue<int?>(8),
                    position);
            }

            return (
                new StreamMessage(
                    streamId.IdOriginal,
                    messageId,
                    streamVersion,
                    position,
                    createdUtc,
                    type,
                    jsonMetadata,
                    ct => GetJsonData(streamId, streamVersion)(ct)),
                reader.GetFieldValue<int?>(8),
                position);
        }
    }
}