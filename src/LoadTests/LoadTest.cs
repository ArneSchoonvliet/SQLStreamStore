namespace LoadTests
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using EasyConsole;
    using SqlStreamStore;

    public abstract class LoadTest
    {
        public abstract Task Run(CancellationToken cancellationToken);

        protected async Task<(IStreamStore, Action, string)> GetStore(CancellationToken cancellationToken)
        {
            IStreamStore streamStore = null;
            IDisposable disposable = null;
            string connectionString = null;

            Output.WriteLine(ConsoleColor.Yellow, "Store type:");
            await new Menu()
                .AddSync("InMem", () => streamStore = new InMemoryStreamStore())
                .Add("Postgres (Docker)",
                    async ct =>
                    {
                        var fixture = new PostgresStreamStoreDb("dbo");
                        Console.WriteLine(fixture.ConnectionString);
                        
                        var gapHandlingInput = Input.ReadString("Use new gap handling (y/n): ");

                        var newGapHandlingEnabled = gapHandlingInput.ToLower() == "y";
                        
                        streamStore = await fixture.GetPostgresStreamStore(newGapHandlingEnabled ? new IntigritiGapHandlingSettings(true, 6000, 12000) : null);
                        disposable = fixture;
                        connectionString = fixture.ConnectionString;
                    })
                .Add("Postgres (Server)",
                    async ct =>
                    {
                        Console.Write("Enter the connection string: ");
                        connectionString = Console.ReadLine();
                        var postgresStreamStoreDb = new PostgresStreamStoreDb("dbo", connectionString);
                        Console.WriteLine(postgresStreamStoreDb.ConnectionString);
                        
                        var gapHandlingInput = Input.ReadString("Use new gap handling (y/n): ");

                        var newGapHandlingEnabled = gapHandlingInput.ToLower() == "y";
                        
                        streamStore = await postgresStreamStoreDb.GetPostgresStreamStore(newGapHandlingEnabled ? new IntigritiGapHandlingSettings(true, 6000, 12000) : null);
                        disposable = postgresStreamStoreDb;
                    })
                .Display(cancellationToken);

            return (
                streamStore,
                () =>
                {
                    streamStore.Dispose();
                    disposable?.Dispose();
                },
                connectionString);
        }
    }
}