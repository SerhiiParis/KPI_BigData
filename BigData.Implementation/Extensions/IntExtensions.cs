using BigData.Implementation.Contracts;

namespace BigData.Implementation.Extensions
{
    public static class IntExtensions
    {
        public static StatusResult ToStatusResult(this int result)
        {
            return result == 0 ? StatusResult.Done : StatusResult.Failure;
        }
    }
}