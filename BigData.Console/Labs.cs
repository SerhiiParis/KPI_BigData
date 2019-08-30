using BigData.Console.Extensions;
using BigData.Console.Factories;
using BigData.Implementation;
using BigData.Implementation.Mappers;
using BigData.Implementation.Reducers;

namespace BigData.Console
{
    public static class Labs
    {
        public static void Lab1()
        {
            var myConfig = ConfigFactory.GetDefault();
            
            var worker = new SimpleWorker<EvenOddMapper, SumReducer>();
            var result = worker.Work(myConfig, out var exitCode);
            
            exitCode.Print();
        }
    }
}