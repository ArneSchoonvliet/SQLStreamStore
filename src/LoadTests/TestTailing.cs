namespace LoadTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;

    using EasyConsole;

    using Microsoft.Data.SqlClient;

    using SqlStreamStore;
    using SqlStreamStore.Streams;

    public class TestTailing : LoadTest
    {
        private static readonly object s_lock = new object();
        private static readonly List<long> s_db = new List<long>();
        private static IAllStreamSubscription s_subscription;
        public override async Task Run(CancellationToken ct)
        {
            Output.WriteLine("");
            Output.WriteLine(ConsoleColor.Green, "Appends events to streams and reads them all back in a single task.");
            Output.WriteLine("");

            var (streamStore, dispose) = await GetStore(ct);

            try
            {
                var numberOfStreams = Input.ReadInt("Number of streams: ", 1, 100000000);
                int messageJsonDataSize = Input.ReadInt("Size of Json (kb): ", 1, 1024);
                int numberOfMessagesPerAmend = Input.ReadInt("Number of messages per stream append: ", 1, 1000);

                int readPageSize = Input.ReadInt("Read page size: ", 1, 10000);

                string jsonData = $@"{{""b"": ""{new string('a', messageJsonDataSize * 1024)}""}}";


                var linkedToken = CancellationTokenSource.CreateLinkedTokenSource(ct);
                var readLikeToken = new CancellationTokenSource();
#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                Task.Run(() => RunSubscribe(streamStore, readPageSize), linkedToken.Token);
                Enumerable.Range(0, 128)
                    .Select(_ => Task.Run(() => RunLike(streamStore, readLikeToken.Token), readLikeToken.Token))
                    .ToList();
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

                var sw = Stopwatch.StartNew();
                var l = new List<(CancellationTokenSource source, Task task)>();
                var maxConcurrent = 0;
                while(sw.ElapsedMilliseconds < TimeSpan.FromMinutes(2).TotalMilliseconds)
                {
                    var head = await streamStore.ReadHeadPosition(linkedToken.Token);

                    lock(s_lock)
                    {
                        var subscriptionPosition = s_db.LastOrDefault();

                        Output.WriteLine($"Head: {head}, Subscription: {subscriptionPosition}");

                        if(head > subscriptionPosition + 1000)
                        {
                            if(l.Any())
                            {
                                Output.WriteLine("Cancelling");
                                var cts = l.Last();
                                cts.source.Cancel();
                                cts.source.Dispose();
                                l.Remove(cts);
                                maxConcurrent--;
                            }
                        }
                        else if(maxConcurrent < 100)
                        {
                            var cts = new CancellationTokenSource();
                            var t = Task.Run(() =>
                                    RunWrites(cts.Token,
                                        numberOfMessagesPerAmend,
                                        numberOfStreams,
                                        l.Count * numberOfStreams,
                                        jsonData,
                                        streamStore),
                                cts.Token);
                            l.Add((cts, t));

                            maxConcurrent++;
                        }
                    }

                    await Task.Delay(200, linkedToken.Token);
                }

                readLikeToken.Cancel();
                readLikeToken.Dispose();

                foreach(var valueTuple in l)
                {
                    valueTuple.source.Cancel();
                    valueTuple.source.Dispose();
                }

                await Task.WhenAll(l.Select(x => x.task).Where(t => !t.IsCanceled));

                Output.WriteLine("Writes finished");


                var db = new List<long>();
                ReadAllPage page = await streamStore.ReadAllForwards(Position.Start, 500, false, ct);
                do
                {
                    db.AddRange(page.Messages.Select(x => x.Position));
                    page = await page.ReadNext(linkedToken.Token);
                } while(!page.IsEnd);

                db.AddRange(page.Messages.Select(x => x.Position));


                while(true)
                {
                    var head = await streamStore.ReadHeadPosition(linkedToken.Token);
                    lock(s_lock)
                    {
                        if(head == s_db.Last())
                        {
                            break;
                        }
                    }

                    await Task.Delay(150, linkedToken.Token);
                }

                linkedToken.Cancel();
                linkedToken.Dispose();
                s_subscription.Dispose();

                lock(s_lock)
                {
                    Output.WriteLine(!db.SequenceEqual(s_db) ? "DONE WITH GAPS" : "Done without gaps");
                }

            }
            finally
            {
                dispose();
            }
        }

        private static async Task RunLike(IStreamStore streamStore, CancellationToken token)
        {
            while(true)
            {
                try
                {
                    token.ThrowIfCancellationRequested();
                    await streamStore.ListStreams(Pattern.StartsWith("stream"), 500, null, token);
                }
                catch(OperationCanceledException ex)
                {
                    Output.WriteLine(ex.ToString());
                    break;
                }
                catch(Exception ex)
                {
                    Output.WriteLine(ex.ToString());
                }
            }
        }

        private static Task RunSubscribe(IStreamStore streamStore, int readPageSize)
        {
            s_subscription = streamStore.SubscribeToAll(
                null,
                (_, m, ___) =>
                {
                    lock (s_lock)
                    {
                        s_db.Add(m.Position);
                        return Task.CompletedTask;
                    }

                });
            s_subscription.MaxCountPerRead = readPageSize;

            return Task.CompletedTask;
        }

        private static async Task RunWrites(
            CancellationToken ct,
            int numberOfMessagesPerAmend,
            int numberOfStreams,
            int offset,
            string jsonData,
            IStreamStore streamStore)
        {
            var stopwatch = Stopwatch.StartNew();
            var messageNumbers = new int[numberOfMessagesPerAmend];
            int count = 1;
            for (int i = 0; i < numberOfStreams; i++)
            {
                try
                {
                    ct.ThrowIfCancellationRequested();

                    for(int j = 0; j < numberOfMessagesPerAmend; j++)
                    {
                        messageNumbers[j] = count++;
                    }

                    var newmessages = MessageFactory
                        .CreateNewStreamMessages(jsonData, messageNumbers);

                    await streamStore.AppendToStream(
                        $"stream-{i + offset}",
                        ExpectedVersion.Any,
                        newmessages,
                        ct);
                    //Console.Write($"> {messageNumbers[numberOfMessagesPerAmend - 1]}");
                }
                catch(SqlException ex) when(ex.Number == -2)
                {
                    // just timeout
                }
                catch(OperationCanceledException ex)
                {
                    Output.WriteLine(ex.ToString());
                    break;
                }
                catch (Exception ex) when (!(ex is OperationCanceledException))
                {
                    Output.WriteLine(ex.ToString());
                    break;
                }
            }
            stopwatch.Stop();
            var rate = Math.Round((decimal)count / stopwatch.ElapsedMilliseconds * 1000, 0);

            Output.WriteLine("");
            Output.WriteLine($"> {count - 1} messages written in {stopwatch.Elapsed} ({rate} m/s)");
        }
    }
}