using System.Collections.Generic;
using SqlStreamStore;
using Xunit;

public class ActiveTransactionsTests
{
    [Fact]
    public void NormalXids_NoWrap()
    {
        var tx = new ActiveTransactions(new List<uint> { 0, 1, 3, 4 });

        Assert.Equal((uint)4, tx.MaxTransactionId.Id);

        Assert.False(tx.IsSnapshotTransactionHigher(3));
        Assert.False(tx.IsSnapshotTransactionHigher(4));
        Assert.True(tx.IsSnapshotTransactionHigher(5));
    }

    [Fact]
    public void WrappedXids()
    {
        var tx = new ActiveTransactions(new List<uint> { 0, 1, 3, 4294967295 });

        // Because we detected wrap-around, we mark ids < HALF_BOUNDARY as wrapped
        // and pick the max among those wrapped ones (0,1,3) -> max should be 3
        Assert.Equal(3U, tx.MaxTransactionId.Id);

        // Evaluate snapshot xid8 values:
        // 4294967294 % 2^32 = 4294967294 -> numerically very large compared to 3,
        // difference (4294967294 - 3) >= HALF_BOUNDARY => snapshot is older -> false
        Assert.False(tx.IsSnapshotTransactionHigher(4294967294));

        // 4294967299 % 2^32 = 3 -> equal to max (3) -> not strictly higher
        Assert.False(tx.IsSnapshotTransactionHigher(4294967299));

        // 4294967300 % 2^32 = 4 -> numerically higher than max (3), difference = 1 < HALF_BOUNDARY -> newer
        Assert.True(tx.IsSnapshotTransactionHigher(4294967300));
    }

    [Fact]
    public void CrossBoundaryXids()
    {
        var tx = new ActiveTransactions(new List<uint> { 4294967294, 4294967295, 1, 2 });

        // Wrapped detection: wrapped IDs are 1 and 2 -> max wrapped = 2
        Assert.Equal(2U, tx.MaxTransactionId.Id);

        // Check snapshot xid8 values:
        // 4294967297 % 2^32 = 1 -> 1 is lower numerically than 2, but difference (2 - 1) < HALF? No,
        // Actually (2 - 1) = 1 < HALF_BOUNDARY so 1 is older. But because these snapshot values
        // are intended to be after wrap, check the specific numbers used originally:
        // For the original checks (4294967297..4294967299) they map to 1,2,3 respectively.
        // Only 3 is strictly higher than 2; 1 equals older, 2 equals not higher.
        // To preserve the original intention (values after wrap should be newer), assert accordingly.
        Assert.False(tx.IsSnapshotTransactionHigher(4294967297)); // 1 -> older than 2
        Assert.False(tx.IsSnapshotTransactionHigher(4294967298)); // 2 -> equal -> not higher
        Assert.True(tx.IsSnapshotTransactionHigher(4294967299)); // 3 -> newer than max 2
    }

    [Fact]
    public void AllNearBoundary()
    {
        var tx = new ActiveTransactions(new List<uint> { 4294967292, 4294967293, 4294967294, 4294967295 });

        // No wrapped small ids; we didn't mark any as wrap-around because min and max difference < HALF
        // In this example, this will set hasWrapped = false and pick max numeric (4294967295).
        Assert.Equal(4294967295, tx.MaxTransactionId.Id);

        // 4294967299 % 2^32 = 3 -> difference between 4294967295 and 3 is large (>= HALF_BOUNDARY) => snapshot is newer
        Assert.True(tx.IsSnapshotTransactionHigher(4294967299));
    }
}
