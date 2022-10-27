namespace SqlStreamStore
{
    public class GapHandlingSettings
    {
        public GapHandlingSettings(bool allowSkip, long possibleDeadlockTime, long possibleAllowedSkipTime)
        {
            AllowSkip = allowSkip;
            PossibleDeadlockTime = possibleDeadlockTime;
            PossibleAllowedSkipTime = possibleAllowedSkipTime;
        }

        /// <summary>
        /// Setting this to 'true' will allow the subscriber the skip a certain position after the PossibleAllowedSkipTime has passed
        /// </summary>
        public bool AllowSkip { get; }

        /// <summary>
        /// The time that needs to pass before we start logging that transactions are taking longer than expected.
        /// </summary>
        public long PossibleDeadlockTime { get; }

        /// <summary>
        /// The time that needs to pass before we allow to move on with subscribing and live with the fact that we might have skipped an event.
        /// </summary>
        public long PossibleAllowedSkipTime { get; }
    }
}