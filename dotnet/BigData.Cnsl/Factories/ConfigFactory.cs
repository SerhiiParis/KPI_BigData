using System.Linq;
using Microsoft.Hadoop.MapReduce;

namespace BigData.Cnsl.Factories
{
    public static class ConfigFactory
    {
        public static HadoopJobConfiguration GetDefault()
        {
            return new HadoopJobConfiguration
            {
                InputPath = "../../../_data/Default/in", 
                OutputFolder = "../../../_data/Default/out"
            }.Required();
        }
        
        public static HadoopJobConfiguration Get(string inputFile)
        {
            if (inputFile[0] == '/')
                inputFile = inputFile.Remove(0, 1);
            
            var @out = inputFile.Split('/').Take(inputFile.Split('/').Length - 1);
            
            return new HadoopJobConfiguration
            {
                InputPath = $"../../../_data/{inputFile}", 
                OutputFolder = $"../../../_data/{@out}out"
            }.Required();
        }

        private static HadoopJobConfiguration Required(this HadoopJobConfiguration config)
        {
            config.FilesToInclude.Add("Microsoft.WindowsAzure.Management.Framework.Threading.dll");
            return config;
        }
    }
}