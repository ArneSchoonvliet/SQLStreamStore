namespace LoadTests
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using Microsoft.Data.SqlClient;
    using SqlStreamStore;
    using SqlStreamStore.Streams;

    public class TestTailing : LoadTest
    {
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

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                Task.Run(() => RunRead(streamStore, readPageSize), linkedToken.Token);
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

                var list = new List<Task>();
                for(int i = 0; i < 30; i++)
                {
                    var t = Task.Run(() =>
                            RunWrites(ct,
                                numberOfMessagesPerAmend,
                                numberOfStreams,
                                i * numberOfStreams,
                                jsonData,
                                streamStore),
                        ct);
                    list.Add(t);
                }

                await Task.WhenAll(list);

                Output.WriteLine("Writes finished");
                linkedToken.Cancel();

                s_subscription.Dispose();

                //await WriteActualGaps(ct, streamStore);

                Output.WriteLine("Done");
            }
            finally
            {
                dispose();
            }
        }

        private static async Task WriteActualGaps(CancellationToken ct, IStreamStore streamStore)
        {
            var stopwatch = Stopwatch.StartNew();
            var count = 0;
            Output.WriteLine("Actual gaps:");
            var page = await streamStore.ReadAllForwards(Position.Start, 73, false, ct);
            count += page.Messages.Length;
            var prevPosition = page.Messages[0].Position;
            for(int i = 1; i < page.Messages.Length; i++)
            {
                if(prevPosition + 1 != page.Messages[i].Position)
                {
                    Output.WriteLine($"- {prevPosition} : {page.Messages[i].Position}");
                }
                prevPosition = page.Messages[i].Position;
            }
            while(!page.IsEnd)
            {
                page = await page.ReadNext(ct);
                count += page.Messages.Length;
                for(int i = 0; i < page.Messages.Length; i++)
                {
                    if(prevPosition + 1 != page.Messages[i].Position)
                    {
                        Output.WriteLine($"- {prevPosition} : {page.Messages[i].Position}");
                    }
                    prevPosition = page.Messages[i].Position;
                }
            }
            stopwatch.Stop();
            var rate = Math.Round((decimal) count / stopwatch.ElapsedMilliseconds * 1000, 0);
            Output.WriteLine("");
            Output.WriteLine($"< {count} messages read {stopwatch.Elapsed} ({rate} m/s)");
        }


        private static Task RunRead(IStreamStore streamStore, int readPageSize)
        {
            s_subscription = streamStore.SubscribeToAll(
                null,
                (_, m, ___) =>
                {
                    s_db.Add(m.Position);
                    return Task.CompletedTask;
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
            for(int i = 0; i < numberOfStreams; i++)
            {
                ct.ThrowIfCancellationRequested();
                try
                {
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
                catch(Exception ex) when(!(ex is TaskCanceledException))
                {
                    Output.WriteLine(ex.ToString());
                }
            }
            stopwatch.Stop();
            var rate = Math.Round((decimal) count / stopwatch.ElapsedMilliseconds * 1000, 0);

            Output.WriteLine("");
            Output.WriteLine($"> {count - 1} messages written in {stopwatch.Elapsed} ({rate} m/s)");
        }

        private enum YesNo
        {
            Yes,
            No
        }
    }
}