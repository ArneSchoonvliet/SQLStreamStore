namespace SqlStreamStore.TestUtils.Postgres
{
    using Npgsql;

    public class PostgresServer : PostgresDatabaseManager
    {
        public PostgresServer(string connectionString) : base(new NpgsqlConnectionStringBuilder(connectionString))
        {
        }
    }
}
