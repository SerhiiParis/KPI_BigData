using BigData.Implementation.Contracts;
using BigData.Implementation.Extensions;
using BigData.Implementation.Factories;
using Microsoft.Hadoop.MapReduce;
using Microsoft.Hadoop.WebClient.WebHCatClient;

namespace BigData.Implementation
{
    public class SimpleWorker<TMapper, TReducer>
        where TMapper : MapperBase, new()
        where TReducer : ReducerCombinerBase, new()
    {
        public MapReduceResult Work(HadoopJobConfiguration config, out StatusResult statusCode)
        {
            var myCluster = ClusterFactory.GetCluster();
            var jobResult = myCluster.MapReduceJob.Execute<TMapper, TReducer>(config);

            statusCode = jobResult.Info.ExitCode.ToStatusResult();
            return jobResult;
        }
    }
}