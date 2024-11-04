namespace SqlStreamStore
{
    using System;

    public class GapHandlingSettings
    {
        public GapHandlingSettings(long minimumWarnTime, long skipTime, uint? safetyTransactionGap = null, int? delayTime = null)
        {
            if(minimumWarnTime >= skipTime)
                throw new ArgumentException("'MinimumWarnTime' is not allowed to be bigger or equal then 'SkipTime'");

            MinimumWarnTime = minimumWarnTime;
            SkipTime = skipTime;
            SafetyTransactionGap = safetyTransactionGap;
            DelayTime = delayTime;
        }

        /// <summary>
        /// The time that needs to pass before we start logging that transactions are taking longer than expected.
        /// </summary>
        public long MinimumWarnTime { get; }

        /// <summary>
        /// The time that needs to pass before we allow to move on with subscribing and live with the fact that we might have skipped an event.
        /// </summary>
        public long SkipTime { get; }

        /// <summary>
        /// If we detect a gap and this option is specified we will wait at least <see cref="DelayTime"/> to recheck if the gap is still there
        /// </summary>
        public uint? SafetyTransactionGap { get; }
        
        /// <summary>
        /// If we detect a gap and <see cref="SafetyTransactionGap"/> is specified we will wait at least this time to recheck if the gap is still there
        /// </summary>
        public int? DelayTime { get; }
    }
}