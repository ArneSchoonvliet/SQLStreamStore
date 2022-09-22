namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Linq;

    internal class TxIdList : List<TransactionInfo>
    {
        public override string ToString()
        {
            return this.Any() ? string.Join("|", this) : "No transaction ids";
        }
    }

    internal class TransactionInfo
    {
        public long Id { get; private set; }
        public string LockMode { get; private set; }

        public TransactionInfo(long id, string lockMode)
        {
            Id = id;
            LockMode = lockMode;
        }
    }
}