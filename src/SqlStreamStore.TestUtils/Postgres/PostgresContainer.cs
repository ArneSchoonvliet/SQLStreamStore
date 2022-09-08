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
        private const string Image = "postgres:9.6";
        private const string ContainerName = "sql-stream-store-tests-postgres";
        private const int Port = 5432;

        public override string ConnectionString => ConnectionStringBuilder.ConnectionString;

        public PostgresContainer(string databaseName)
            : base(databaseName)
        {
            _containerService = new Builder()
                .UseContainer()
                .WithName(ContainerName)
                .UseImage(Image)
                .KeepRunning()
                .ReuseIfExists()
                .WithEnvironment("POSTGRES_PASSWORD=password")
                .ExposePort(Port, Port)
                .Command("", "-c max_connections=1000 -c shared_buffers=512MB")
                .Build();
        }

        public async Task Start(CancellationToken cancellationToken = default)
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

        private NpgsqlConnectionStringBuilder ConnectionStringBuilder => new NpgsqlConnectionStringBuilder
        {
            Database = DatabaseName,
            Password = "password",
            Port = Port,
            Username = "postgres",
            Host = "localhost",
            Pooling = true,
            MaxPoolSize = 1024
        };
    }
}
