using Microsoft.Hadoop.MapReduce;

namespace BigData.Implementation.Mappers
{
    public class EvenOddMapper : MapperBase
    {
        public override void Map(string inputLine, MapperContext context)
        {
            var value = int.Parse(inputLine);

            var key = value % 2 == 0 ? "even" : "odd";

            context.EmitKeyValue(key, value.ToString());
        }
    }
}