using System.Linq;
using Microsoft.Hadoop.MapReduce;

namespace BigData.Console.Factories
{
    public static class ConfigFactory
    {
        public static HadoopJobConfiguration GetDefault()
        {
            return new HadoopJobConfiguration
            {
                InputPath = "../../../_data/Default/in", 
                OutputFolder = "../../../_data/Default/out"
            };
        }
        
        public static HadoopJobConfiguration Get(string inputFile)
        {
            if (inputFile[0] == '/')
                inputFile = inputFile.Remove(0, 1);
            
            var @out = inputFile.Split("/").Take(inputFile.Split("/").Length - 1);
            
            return new HadoopJobConfiguration
            {
                InputPath = $"../../../_data/{inputFile}", 
                OutputFolder = $"../../../_data/{@out}out"
            };
        }
    }
}