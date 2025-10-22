namespace SqlStreamStore
{
    using System.Collections.Generic;
    using System.Linq;
    
    public class CurrentTransactions : List<uint>
    {
        public override string ToString()
        {
            var inProgress = Count > 0;
            return inProgress 
                ? string.Join(", ", this.OrderBy(x => x).Select(x => $"TransactionId: {x}")) 
                : "No transactions in progress";
        }
    }
}