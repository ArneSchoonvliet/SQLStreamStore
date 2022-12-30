namespace SqlStreamStore
{
    using Npgsql;
    using SqlStreamStore.Imports.Ensure.That;
    using SqlStreamStore.Infrastructure;
    using SqlStreamStore.Subscriptions;

    public class PostgresStreamStoreSettings
    {
        private string _schema = "public";
        private GetUtcNow _getUtcNow = SystemClock.GetUtcNow;

        /// <summary>
        /// Initializes a new instance of <see cref="PostgresStreamStoreSettings"/>.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="gapHandlingSettings">Settings that are used for gap handling</param>
        public PostgresStreamStoreSettings(string connectionString,GapHandlingSettings gapHandlingSettings = null)
            : this(new NpgsqlDataSourceBuilder(connectionString), gapHandlingSettings)
        {
            Ensure.That(connectionString, nameof(connectionString)).IsNotNullOrWhiteSpace();
        }

        /// <summary>
        /// Initializes a new instance of <see cref="PostgresStreamStoreSettings"/>.
        /// </summary>
        /// <param name="npgsqlDataSourceBuilder">The NpgsqlDataSourceBuilder.</param>
        /// <param name="gapHandlingSettings">Settings that are used for gap handling</param>
        public PostgresStreamStoreSettings(
            NpgsqlDataSourceBuilder npgsqlDataSourceBuilder,
            GapHandlingSettings gapHandlingSettings = null)
        {
            Ensure.That(npgsqlDataSourceBuilder, nameof(npgsqlDataSourceBuilder)).IsNotNull();

            NpgsqlDataSourceBuilder = npgsqlDataSourceBuilder;
            GapHandlingSettings = gapHandlingSettings;
        }

        /// <summary>
        ///     Allows overriding the way a <see cref="NpgsqlDataSource"/> is created by passing your own <see cref="NpgsqlDataSourceBuilder"/>.
        ///     The default implementation simply passes the connection string into the <see cref="NpgsqlDataSourceBuilder"/> constructor.
        /// </summary>
        public NpgsqlDataSourceBuilder NpgsqlDataSourceBuilder { get; }

        /// <summary>
        ///    Settings that are used for gap handling.
        /// </summary>
        public GapHandlingSettings GapHandlingSettings { get; }

        /// <summary>
        ///     Allows overriding of the stream store notifier. The default implementation
        ///     creates <see cref="PollingStreamStoreNotifier"/>.
        /// </summary>
        public CreateStreamStoreNotifier CreateStreamStoreNotifier { get; set; } =
            store => new PollingStreamStoreNotifier(store);

        /// <summary>
        ///     The schema SQL Stream Store should place database objects into. Defaults to "public".
        /// </summary>
        public string Schema
        {
            get => _schema;
            set
            {
                Ensure.That(value, nameof(Schema)).IsNotNullOrWhiteSpace();

                _schema = value;
            }
        }

        /// <summary>
        ///     Loads the auto_explain module and turns it on for all queries. Useful for index tuning.
        /// </summary>
        public bool ExplainAnalyze { get; set; }

        /// <summary>
        ///     A delegate to return the current UTC now. Used in testing to
        ///     control timestamps and time related operations.
        /// </summary>
        public GetUtcNow GetUtcNow
        {
            get => _getUtcNow;
            set => _getUtcNow = value ?? SystemClock.GetUtcNow;
        }

        /// <summary>
        ///     The log name used for any of the log messages.
        /// </summary>
        public string LogName { get; } = nameof(PostgresStreamStore);

        /// <summary>
        ///     Allows setting whether or not deleting expired (i.e., older than maxCount) messages happens in the same database transaction as append to stream or not.
        ///     This does not effect scavenging when setting a stream's metadata - it is always run in the same transaction.
        /// </summary>
        public bool ScavengeAsynchronously { get; set; } = true;

        /// <summary>
        ///     Disables stream and message deletion tracking. Will increase
        ///     performance, however subscribers won't know if a stream or a
        ///     message has been deleted. This can be modified at runtime.
        /// </summary>
        public bool DisableDeletionTracking { get; set; }
    }
}