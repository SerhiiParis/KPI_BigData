using Microsoft.Hadoop.MapReduce;

namespace BigData.Implementation.Mappers
{
    public class WordCountMapper : MapperBase
    {
        public override void Map(string inputLine, MapperContext context)
        {
            var words = inputLine.Split(' ', '\n', '\t');

            foreach (var word in words)
                context.EmitKeyValue(word, 1.ToString());
        }
    }
}