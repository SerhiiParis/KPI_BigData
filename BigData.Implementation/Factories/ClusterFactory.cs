using System;
using Microsoft.Hadoop.MapReduce;
using Microsoft.Hadoop.MapReduce.HadoopImplementations;
using Microsoft.Hadoop.MapReduce.HdfsExtras.Hdfs;
using Microsoft.Hadoop.WebClient.WebHCatClient;

namespace BigData.Implementation.Factories
{
    public static class ClusterFactory
    {
        public static IHadoop GetCluster()
        {
            return Hadoop.Connect(new Uri("http://localhost"), "hadoop", null);
        }
    }
}