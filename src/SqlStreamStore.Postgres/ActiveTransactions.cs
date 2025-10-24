namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Represents a collection of PostgreSQL transaction IDs (32-bit <c>xid</c>)
    /// and provides logic for comparing 64-bit <c>xid8</c> snapshot IDs against
    /// the currently active 32-bit <c>xid</c> values, taking into account
    /// PostgreSQL’s circular wrap-around behavior (modulo 2^32).
    /// </summary>
    public class ActiveTransactions
    {
        private const ulong WRAP_BOUNDARY = 4294967296UL; // 2^32
        private const uint HALF_BOUNDARY = 2147483648U; // 2^31

        /// <summary>
        /// Gets the list of active transactions as 32-bit <c>xid</c> values.
        /// </summary>
        public List<ActiveTransaction> TransactionIds { get; }

        /// <summary>
        /// Gets the highest (most recent) active transaction ID (<c>xid</c>)
        /// in the current set, accounting for possible wrap-around.
        /// May be <see langword="null"/> if no active transactions are present.
        /// </summary>
        public ActiveTransaction MaxTransactionId { get; }
        
        /// <summary>
        /// Gets a value indicating whether there are no active transactions in this set.
        /// </summary>
        public bool NoActiveTransactions { get; }

        /// <summary>
        /// Initializes a new instance of <see cref="ActiveTransactions"/> with
        /// the given list of 32-bit <c>xid</c> values.
        /// Automatically detects whether the provided transaction IDs have
        /// wrapped around the 2^32 boundary.
        /// </summary>
        /// <param name="transactions">
        /// A list of active transaction IDs (32-bit <c>xid</c> values),
        /// or an empty list if there are no active transactions.
        /// </param>
        public ActiveTransactions(List<uint> transactions)
        {
            if(transactions == null || transactions.Count == 0)
            {
                TransactionIds = new List<ActiveTransaction>();
                MaxTransactionId = null;
                NoActiveTransactions = true;
                return;
            }

            var maxValue = transactions.Max();
            var minValue = transactions.Min();
            var hasWrapped = maxValue - minValue >= HALF_BOUNDARY;

            if(!hasWrapped)
            {
                TransactionIds = transactions
                    .Select(x => new ActiveTransaction(x, false))
                    .ToList();
                MaxTransactionId = TransactionIds.MaxBy(x => x.Id);
            }
            else
            {
                // Mark transactions below HALF_BOUNDARY as "wrapped"
                TransactionIds = transactions
                    .Select(x => new ActiveTransaction(x, x < HALF_BOUNDARY))
                    .ToList();
                MaxTransactionId = TransactionIds
                    .Where(x => x.WrapAround)
                    .MaxBy(x => x.Id);
            }
        }

        /// <summary>
        /// Determines whether this set of transactions shares any common
        /// transaction IDs with another set.
        /// </summary>
        /// <param name="other">The other set of active transactions to check against.</param>
        /// <returns>
        /// <see langword="true"/> if there is at least one transaction ID common
        /// to both sets; otherwise <see langword="false"/>.
        /// </returns>
        public bool SharesTransactionsWith(ActiveTransactions other)
        {
            return TransactionIds.Intersect(other.TransactionIds).Any();
        }

        /// <summary>
        /// Determines if a 64-bit snapshot transaction ID (<c>xid8</c>) represents
        /// a transaction that is strictly newer (greater) than the newest active
        /// 32-bit <c>xid</c> in this set, using PostgreSQL’s circular transaction ID rules.
        /// </summary>
        /// <param name="snapshotTransactionId">
        /// The 64-bit absolute transaction ID (<c>xid8</c>) of the snapshot.
        /// </param>
        /// <returns>
        /// <see langword="true"/> if the snapshot’s <c>xid8</c> is strictly newer
        /// than the newest active <c>xid</c> in the set; otherwise <see langword="false"/>.
        /// Always <see langword="true"/> if there are no active transactions.
        /// </returns>
        public bool IsSnapshotTransactionHigher(ulong snapshotTransactionId)
        {
            if(MaxTransactionId == null)
                return true; // If no transactions, any snapshot is considered newer.

            // 1. Convert the ulong snapshot ID to its wrapped uint counterpart (0 to WRAP_BOUNDARY - 1).
            // The modulo uses the ulong WRAP_BOUNDARY.
            var wrappedSnapshotId = (uint)(snapshotTransactionId % WRAP_BOUNDARY); // A

            // 2. Get the Max active transaction ID (B) from the current set. 
            var maxActiveId = MaxTransactionId.Id; // B

            // If IDs are equal, the Xmin is not strictly higher.
            if(wrappedSnapshotId == maxActiveId)
                return false;

            // The difference (distance) is now calculated directly using uints, 
            // as the maximum possible difference (2^32 - 1) fits within a uint.

            // Case A: Snapshot ID (A) is numerically HIGHER than Max Active ID (B).
            if(wrappedSnapshotId > maxActiveId)
            {
                var difference = wrappedSnapshotId - maxActiveId;

                // If the difference is small (< HALF_BOUNDARY), A is newer. 
                // If the difference is large (>= HALF_BOUNDARY), B must have wrapped, making A older.
                return difference < HALF_BOUNDARY;
            }
            else // Case B: Snapshot ID (A) is numerically LOWER than Max Active ID (B).
            {
                var difference = maxActiveId - wrappedSnapshotId;

                // If the difference is large (>= HALF_BOUNDARY), A must have wrapped, making A newer.
                // If the difference is small (< HALF_BOUNDARY), A is simply older than B.
                return difference >= HALF_BOUNDARY;
            }
        }

        public override string ToString()
        {
            var inProgress = TransactionIds.Count > 0;
            return inProgress
                ? string.Join(", ", TransactionIds.OrderBy(x => x.WrapAround).ThenBy(x => x.Id).Select(x => x))
                : "No transactions in progress";
        }
    }

    /// <summary>
    /// Represents a single PostgreSQL transaction ID (32-bit <c>xid</c>),
    /// optionally flagged if it has wrapped around the 2^32 boundary.
    /// </summary>
    public class ActiveTransaction
    {
        public ActiveTransaction(uint id, bool wrapAround)
        {
            Id = id;
            WrapAround = wrapAround;
        }

        public uint Id { get; }
        public bool WrapAround { get; }

        public override string ToString()
        {
            return $"TransactionId: {Id}";
        }
    }
}