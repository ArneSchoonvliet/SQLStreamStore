﻿namespace SqlStreamStore
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;

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

            maxCount = maxCount == int.MaxValue ? maxCount - 1 : maxCount;

            var (messages, transactionSnapshot) = await ReadAllPageForwards(fromPositionExclusive, null, maxCount, prefetch, cancellationToken);

            if (!messages.Any())
            {
                return new ReadAllPage(
                    fromPositionExclusive,
                    fromPositionExclusive,
                    true,
                    ReadDirection.Forward,
                    readNext,
                    Array.Empty<StreamMessage>());
            }

            messages = await HandleGaps(messages, transactionSnapshot, fromPositionExclusive, maxCount, prefetch, cancellationToken);

            bool isEnd = true;

            if (messages.Count == maxCount + 1) // An extra row was read, we're not at the end
            {
                isEnd = false;
                messages.RemoveAt(maxCount);
            }

            var filteredMessages = FilterExpired(messages);

            var nextPosition = filteredMessages[filteredMessages.Count - 1].Position + 1;

            return new ReadAllPage(
                fromPositionExclusive,
                nextPosition,
                isEnd,
                ReadDirection.Forward,
                readNext,
                filteredMessages.ToArray());

            //using(var connection = await OpenConnection(cancellationToken))
            //using(var transaction = connection.BeginTransaction())
            //using(var command = BuildFunctionCommand(
            //    _schema.ReadAll,
            //    transaction,
            //    Parameters.Count(maxCount + 1),
            //    Parameters.Position(fromPositionExclusive),
            //    Parameters.ReadDirection(ReadDirection.Forward),
            //    Parameters.Prefetch(prefetch)))
            //using(var reader = await command
            //    .ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken)
            //    .ConfigureAwait(false))
            //{
            //    if(!reader.HasRows)
            //    {
            //        return new ReadAllPage(
            //            fromPositionExclusive,
            //            fromPositionExclusive,
            //            true,
            //            ReadDirection.Forward,
            //            readNext,
            //            Array.Empty<StreamMessage>());
            //    }

            //    var messages = new List<(StreamMessage message, int? maxAge)>();

            //    while(await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            //    {
            //        if(messages.Count == maxCount)
            //        {
            //            messages.Add(default);
            //        }
            //        else
            //        {
            //            var streamIdInfo = new StreamIdInfo(reader.GetString(0));
            //            var (message, maxAge, _) =
            //                await ReadAllStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch);
            //            messages.Add((message, maxAge));
            //        }
            //    }

            //    bool isEnd = true;

            //    if(messages.Count == maxCount + 1) // An extra row was read, we're not at the end
            //    {
            //        isEnd = false;
            //        messages.RemoveAt(maxCount);
            //    }

            //    var filteredMessages = FilterExpired(messages);

            //    var nextPosition = filteredMessages[filteredMessages.Count - 1].Position + 1;

            //    return new ReadAllPage(
            //        fromPositionExclusive,
            //        nextPosition,
            //        isEnd,
            //        ReadDirection.Forward,
            //        readNext,
            //        filteredMessages.ToArray());
            //}
        }

        private async Task<(List<(StreamMessage message, int? maxAge)> Messages, string TransactionSnapshot)> ReadAllPageForwards(
            long fromPositionExclusive,
            long? toPositionInclusive,
            int maxCount,
            bool prefetch,
            CancellationToken cancellationToken)
        {
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
                        return (new List<(StreamMessage, int?)>(), null);
                    }

                    var messages = new List<(StreamMessage message, int? maxAge)>();

                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        // TODO: Check why the hell this thing is here
                        //if (messages.Count == maxCount)
                        //{
                        //    messages.Add(default);
                        //}
                        //else
                        //{
                            var streamIdInfo = new StreamIdInfo(reader.GetString(0));
                            var (message, maxAge, _) =
                                await ReadAllStreamMessage(reader, streamIdInfo.PostgresqlStreamId, prefetch);
                            messages.Add((message, maxAge));
                        //}
                    }

                    string transactionSnapshot = string.Empty;
                    await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        transactionSnapshot = await reader.GetFieldValueAsync<string>(0, cancellationToken);
                    }

                    return (messages, transactionSnapshot);
                }
            }
        }

        private async Task<List<(StreamMessage message, int? maxAge)>> HandleGaps(
            List<(StreamMessage message, int? maxAge)> messages,
            string transactionSnapshot,
            long fromPositionInclusive,
            int maxCount,
            bool prefetch,
            CancellationToken cancellationToken)
        {
            var correlation = Guid.NewGuid();
            var transactions = ParseTransactionSnapshot(transactionSnapshot);
            if(!transactions.Any())
            {
                Logger.InfoFormat($"Transaction snapshot {transactionSnapshot} has no other pending transactions. No need for gap checking, all gaps are expected gaps.");
                return messages;
            }

            Logger.InfoFormat("{correlation} Gap checking. Message count: {messageCount} | Transaction snapshot {transactionSnapshot}", correlation, messages.Count, transactionSnapshot);
            for (int i = 0; i < messages.Count - 1; i++)
            {
                var expectedNextPosition = messages[i].message.Position + 1;
                var actualPosition = messages[i + 1].message.Position;
                Logger.InfoFormat("{correlation} Gap checking. Expected position: {expectedNextPosition} | Actual position: {actualPosition}", correlation,expectedNextPosition, actualPosition);

                if (expectedNextPosition != actualPosition)
                {
                    Logger.InfoFormat("{correlation} Gap detected", correlation);
                    var toPositionInclusive = messages[messages.Count - 1].message.Position;

                    Logger.InfoFormat("{correlation} Transaction snapshot {transactionSnapshot} has pending transactions so gaps might be filled. Query partial page from {fromPositionInclusive} to {toPositionInclusive}", correlation, transactionSnapshot, fromPositionInclusive, toPositionInclusive);

                    (List<(StreamMessage message, int? maxAge)> Messages, string TransactionSnapshot) result;
                    do
                    {
                        result = await ReadAllPageForwards(fromPositionInclusive,
                            toPositionInclusive,
                            maxCount,
                            prefetch,
                            cancellationToken);

                        Logger.InfoFormat("{correlation} Transaction snapshot {{transactionSnapshot}} | Re-queried page transaction snapshot {transactionSnapshot}", correlation, transactionSnapshot, result.TransactionSnapshot);

                    } while (transactions.Intersect(ParseTransactionSnapshot(result.TransactionSnapshot)).Any());

                    return result.Messages;

                    // switched this to return the partial page, then re-issue load starting at gap
                    // this speeds up the retry instead of taking a 3 second delay immediately
                    //var messagesBeforeGap = new StreamMessage[i+1];
                    //page.Messages.Take(i+1).ToArray().CopyTo(messagesBeforeGap, 0);
                    //return new ReadAllPage(page.FromPosition, maxPosition, page.IsEnd, page.Direction, ReadNext, messagesBeforeGap);
                }
            }

            return messages;
        }

        private static List<long> ParseTransactionSnapshot(string transactionSnapshot)
        {
            if (!string.IsNullOrWhiteSpace(transactionSnapshot))
            {
                var splitResult = transactionSnapshot.Split(':');

                if (splitResult.Length > 2 && !string.IsNullOrWhiteSpace(splitResult[2]))
                {
                    return splitResult[2].Split(',').Select(x => Convert.ToInt64(x)).ToList();
                }
            }

            return new List<long>();
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