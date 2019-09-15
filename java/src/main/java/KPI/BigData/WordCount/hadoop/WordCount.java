package KPI.BigData.WordCount.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class WordCount {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static class Worker {
         public static void Start(String input, String output) throws IOException {
             JobConf conf = new JobConf(WordCount.class);
             conf.setJobName("wordcount");

             conf.setOutputKeyClass(Text.class);
             conf.setOutputValueClass(IntWritable.class);

             conf.setMapperClass(WordCount.Map.class);
             conf.setCombinerClass(WordCount.Reduce.class);
             conf.setReducerClass(WordCount.Reduce.class);

             conf.setInputFormat(TextInputFormat.class);
             conf.setOutputFormat(TextOutputFormat.class);

             FileInputFormat.setInputPaths(conf, new Path(input));
             FileOutputFormat.setOutputPath(conf, new Path(output));

             JobClient.runJob(conf);
         }
    }
}
