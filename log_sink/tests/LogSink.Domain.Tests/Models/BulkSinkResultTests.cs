using LogSink.Domain.Models;

namespace LogSink.Domain.Tests.Models;

public class BulkSinkResultTests
{
    [Fact]
    public void RequestUnitsConsumed_DefaultsToZero()
    {
        var result = new BulkSinkResult(
            TotalProcessed: 10,
            TotalSuccessful: 9,
            TotalFailed: 1,
            TotalDlqSent: 1,
            ElapsedMilliseconds: 12.5);

        Assert.Equal(0.0, result.RequestUnitsConsumed);
        Assert.Equal(10, result.TotalProcessed);
    }
}
