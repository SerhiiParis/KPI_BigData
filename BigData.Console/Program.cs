using BigData.Console.Extensions;
using BigData.Console.Factories;
using BigData.Implementation;
using BigData.Implementation.Mappers;
using BigData.Implementation.Reducers;

namespace BigData.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            var myConfig = ConfigFactory.GetDefault();
            
            var worker = new SimpleWorker<EvenOddMapper, SumReducer>();
            var result = worker.Work(myConfig, out var exitCode);
            
            exitCode.Print();
            System.Console.ReadKey();
        }
    }
}