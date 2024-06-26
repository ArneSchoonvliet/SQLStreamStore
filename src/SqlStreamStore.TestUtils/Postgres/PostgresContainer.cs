namespace SqlStreamStore.TestUtils.Postgres
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Ductus.FluentDocker.Builders;
    using Ductus.FluentDocker.Services;
    using Npgsql;
    using Polly;

    public class PostgresContainer : PostgresDatabaseManager
    {
        private readonly IContainerService _containerService;
        private const int Port = 5432;

        public PostgresContainer(string databaseName)
            : base(new NpgsqlConnectionStringBuilder
            {
                Database = databaseName,
                Password = "password",
                Port = Port,
                Username = "postgres",
                Host = "127.0.0.1",
                Pooling = true
            })
        {
            _containerService = new Builder()
                .UseContainer()
                .WithName("sql-stream-store-tests-postgres")
                .UseImage("postgres:14-alpine")
                .KeepRunning()
                .ReuseIfExists()
                .WithEnvironment("POSTGRES_PASSWORD=password")
                .ExposePort(Port, Port)
                .Command("", "-c shared_buffers=128MB")
                .Build();
        }

        public override async Task CreateDatabase(CancellationToken cancellationToken = default)
        {
            await Start(cancellationToken);
            await base.CreateDatabase(cancellationToken);
        }

        private async Task Start(CancellationToken cancellationToken = default)
        {
            _containerService.Start();

            await Policy
                .Handle<NpgsqlException>()
                .WaitAndRetryAsync(30, _ => TimeSpan.FromMilliseconds(500))
                .ExecuteAsync(async () =>
                {
                    using(var connection = new NpgsqlConnection(DefaultConnectionString))
                    {
                        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
                    }
                });
        }
    }
}