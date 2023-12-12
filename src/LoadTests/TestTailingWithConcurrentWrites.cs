namespace LoadTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using Npgsql;
    using SqlStreamStore;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.PgSqlScripts;
    using SqlStreamStore.Streams;
    using SqlStreamStore.Subscriptions;

    public class TestTailingWithConcurrentWrites : LoadTest
    {
        private static readonly object s_lock = new();
        private static readonly List<long> s_db = new();
        private static IAllStreamSubscription s_subscription;

        public override async Task Run(CancellationToken ct)
        {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green, "Subscription that tails head with concurrent appends.");
            Output.WriteLine("");

            const string schemaName = "tailing";
            var (streamStore, dispose) = await GetStore(ct, schemaName);

            if(streamStore is not PostgresStreamStore pgStreamStore)
            {
                Output.WriteLine($"No support for {nameof(streamStore)} for this test.");
                return;
            }

            try
            {
                var maxConcurrentWriters = Input.ReadInt("Max concurrent writers: ", 1, 1000);
                var numberOfMessages = Input.ReadInt("Number of messages: ", 1000, 100000000);
                int messageJsonDataSize = Input.ReadInt("Size of Json (kb): ", 1, 1024);
                int readPageSize = Input.ReadInt("Read page size: ", 1, 10000);

                string jsonData = $@"{{""b"": ""{new string('a', messageJsonDataSize * 1024)}""}}";

                await RunSubscribe(pgStreamStore, readPageSize);
                await Task.Delay(10000, ct);

                var messageIterations = Enumerable.Repeat(0, numberOfMessages);
                var options = new ParallelOptions { MaxDegreeOfParallelism = maxConcurrentWriters };

                await Parallel.ForEachAsync(messageIterations,
                    options,
                    async (_, cancellationToken) =>
                    {
                        try
                        {
                            await AddWrite(pgStreamStore, schemaName, jsonData, cancellationToken);
                        }
                        catch
                        {
                        }
                    });

                Output.WriteLine("Writes finished");

                var db = new List<long>();
                ReadAllPage page = await pgStreamStore.ReadAllForwards(Position.Start, 500, false, ct);
                do
                {
                    db.AddRange(page.Messages.Select(x => x.Position));
                    page = await page.ReadNext(ct);
                } while(!page.IsEnd);

                db.AddRange(page.Messages.Select(x => x.Position));

                var sw = Stopwatch.StartNew();

                // It's possible that s_db.Last() will never be equal to head (skipped event) hence why we time limit this loop.
                var maxLoopTime = TimeSpan.FromMinutes(5).TotalMilliseconds;
                while(sw.ElapsedMilliseconds < maxLoopTime)
                {
                    var head = await pgStreamStore.ReadHeadPosition(ct);
                    lock(s_lock)
                    {
                        if(s_db.Any() && head == s_db.Last())
                        {
                            Output.WriteLine("Subscriber is caught up");
                            break;
                        }
                    }

                    await Task.Delay(500, ct);
                }

                s_subscription.Dispose();
                Output.WriteLine("Stopped subscribing");

                lock(s_lock)
                {
                    var skippedEvents = !db.SequenceEqual(s_db);
                    Output.WriteLine(skippedEvents ? "DONE WITH SKIPPED EVENT" : "Done without skipped event");

                    if(skippedEvents)
                    {
                        var missingPositions = db.Except(s_db);
                        foreach(var missingPosition in missingPositions)
                        {
                            Output.WriteLine($"Skipped event {missingPosition}");
                        }
                    }
                }
            }
            finally
            {
                dispose();
            }
        }

        private static Task RunSubscribe(IReadonlyStreamStore streamStore, int readPageSize)
        {
            s_subscription = streamStore.SubscribeToAll(
                null,
                (_, m, ___) =>
                {
                    lock(s_lock)
                    {
                        s_db.Add(m.Position);
                        return Task.CompletedTask;
                    }
                },
                (_, reason, exception) =>
                {
                    if(reason is SubscriptionDroppedReason.Disposed)
                        return;
                    
                    Output.WriteLine("Subscriber crashed! Exception {exception}", exception);
                    Environment.Exit(1);
                });
            
            s_subscription.MaxCountPerRead = readPageSize;
            return s_subscription.Started;
        }

        private static async Task AddWrite(
            PostgresStreamStore pgStreamStore,
            string schemaName,
            string jsonData,
            CancellationToken cancellationToken)
        {
            try
            {
                const int expectedVersion = ExpectedVersion.Any;
                var streamId = $"TransactionTest/{Guid.NewGuid()}";
                var schema = new Schema(schemaName);

                var streamIdInfo = new StreamIdInfo(streamId);
                var newMessages = MessageFactory.CreateNewStreamMessages(jsonData, 1);

                using(var connection = await pgStreamStore.OpenConnection(cancellationToken))
                using(var transaction = await connection.BeginTransactionAsync(cancellationToken))
                using(var command = BuildFunctionCommand(
                          schema.AppendToStream,
                          transaction,
                          Parameters.StreamId(streamIdInfo.PostgresqlStreamId),
                          Parameters.StreamIdOriginal(streamIdInfo.PostgresqlStreamId),
                          Parameters.MetadataStreamId(streamIdInfo.MetadataPosgresqlStreamId),
                          Parameters.ExpectedVersion(expectedVersion),
                          Parameters.CreatedUtc(SystemClock.GetUtcNow.Invoke()),
                          Parameters.NewStreamMessages(newMessages)))
                {
                    try
                    {
                        using(var reader = await command
                                  .ExecuteReaderAsync(cancellationToken)
                                  .ConfigureAwait(false))
                        {
                            await reader.ReadAsync(cancellationToken).ConfigureAwait(false);
                        }


                        await Task.Delay(Random.Shared.Next(0, 10000), cancellationToken);
                        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch(PostgresException ex) when(ex.IsWrongExpectedVersion())
                    {
                        await transaction.RollbackAsync(cancellationToken).ConfigureAwait(false);

                        throw new WrongExpectedVersionException(
                            ErrorMessages.AppendFailedWrongExpectedVersion(streamIdInfo.PostgresqlStreamId.IdOriginal,
                                expectedVersion),
                            streamIdInfo.PostgresqlStreamId.IdOriginal,
                            expectedVersion,
                            ex);
                    }
                }
            }
            catch(Exception ex)
            {
                Output.WriteLine(ex.ToString());
            }
        }

        private static NpgsqlCommand BuildFunctionCommand(
            string function,
            NpgsqlTransaction transaction,
            params NpgsqlParameter[] parameters)
        {
            var command = new NpgsqlCommand(function, transaction.Connection, transaction);

            foreach(var parameter in parameters)
            {
                command.Parameters.Add(parameter);
            }

            command.BuildFunction();
            return command;
        }
    }
}