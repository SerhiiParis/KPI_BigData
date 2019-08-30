using System;
using Microsoft.Hadoop.MapReduce;

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