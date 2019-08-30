using System.Collections.Generic;
using Microsoft.Hadoop.MapReduce;

namespace BigData.Implementation.Reducers
{
    public class SumReducer : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> values, ReducerCombinerContext context)
        {
            int myCount = 0;
            int mySum = 0;

            foreach (string value in values)
            {
                mySum += int.Parse(value);
                myCount++;
            }

            context.EmitKeyValue(key, myCount + "t" + mySum);
        }
    }
}