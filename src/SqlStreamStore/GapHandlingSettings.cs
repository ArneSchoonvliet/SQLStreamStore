namespace SqlStreamStore
{
    using System;

    public class GapHandlingSettings
    {
        public GapHandlingSettings(
            long minimumWarnTime,
            long skipTime,
            uint safetyGap = 500,
            int initialPollingDelay = 100,
            int pollingBackoffIncrement = 25,
            int pollingBackoffIntervalCount = 5)
        {
            if(minimumWarnTime >= skipTime)
                throw new ArgumentException("'MinimumWarnTime' is not allowed to be bigger or equal then 'SkipTime'");

            MinimumWarnTime = minimumWarnTime;
            SkipTime = skipTime;
            SafetyGap = safetyGap;
            InitialPollingDelay = initialPollingDelay;
            PollingBackoffIncrement = pollingBackoffIncrement;
            PollingBackoffIntervalCount = pollingBackoffIntervalCount;
        }

        /// <summary>
        /// The time in milliseconds that needs to pass before we start logging warnings that transactions
        /// are taking longer than expected. Used to detect potential deadlocks or slow transactions.
        /// Must be less than <see cref="SkipTime"/>.
        /// </summary>
        public long MinimumWarnTime { get; }

        /// <summary>
        /// The maximum time in milliseconds to wait for gap-filling transactions to complete.
        /// After this timeout, the system will proceed despite potential gaps, logging an error
        /// about a possible skipped event. This prevents indefinite blocking on stuck transactions.
        /// </summary>
        public long SkipTime { get; }

        /// <summary>
        /// The safety buffer added to transaction IDs when checking against PostgreSQL's Xmin
        /// (oldest visible transaction). A gap is considered permanent (from a rolled-back transaction)
        /// only if: maximumTransactionId + SafetyGap is lower than Xmin. 
        /// Default: 500. Higher values are more conservative but may cause unnecessary polling.
        /// </summary>
        public uint SafetyGap { get; }

        /// <summary>
        /// The initial delay in milliseconds between polling attempts when waiting for transactions
        /// to complete. This delay increases over time via exponential backoff.
        /// Default: 100ms.
        /// </summary>
        public int InitialPollingDelay { get; }

        /// <summary>
        /// The amount in milliseconds to increase the polling delay each time the backoff interval
        /// is reached. For example, with a 25ms increment, delays progress: 100ms → 125ms → 150ms.
        /// Default: 25ms.
        /// </summary>
        public int PollingBackoffIncrement { get; }

        /// <summary>
        /// The number of polling iterations before increasing the delay. For example, with an interval
        /// of 5, the delay increases after every 5th poll attempt (after iterations 5, 10, 15, etc.).
        /// Default: 5.
        /// </summary>
        public int PollingBackoffIntervalCount { get; }
    }
}