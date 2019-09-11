using System.Collections.Generic;
using System.Linq;
using Microsoft.Hadoop.MapReduce;

namespace BigData.Implementation.Reducers
{
    public class SumReducer : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            var mySum = values.Sum(int.Parse);
            context.EmitKeyValue(key, mySum.ToString());
        }
    }
}