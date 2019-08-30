using System;
using BigData.Implementation.Contracts;

namespace BigData.Cnsl.Extensions
{
    public static class PrintExtensions
    {
        public static void Print(this StatusResult data)
        {
            switch (data)
            {
                case StatusResult.Done: System.Console.ForegroundColor = ConsoleColor.Green;
                    break;
                case StatusResult.Failure: System.Console.ForegroundColor = ConsoleColor.Red;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(data), data, null);
            }
            
            Console.WriteLine($"Exit Code: {data.ToString()}");
            Console.ResetColor();
        }
    }
}