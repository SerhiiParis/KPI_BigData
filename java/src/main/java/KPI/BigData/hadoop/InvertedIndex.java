package KPI.BigData.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class InvertedIndex {
    public static class InvertedIndexMapper extends
            Mapper<Object,Text,Object,Text>{
        private Text keyInfo = new Text();
        private  Text valueInfo = new Text();
        private FileSplit split;
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            split = (FileSplit)context.getInputSplit();
            StringTokenizer itr = new StringTokenizer(value.toString());

            while(itr.hasMoreTokens()){
                keyInfo.set(itr.nextToken()+":"+split.getPath().toString());

                int possition = value.toString().indexOf(itr.nextToken());
                valueInfo.set(String.valueOf(possition));

                context.write(keyInfo, valueInfo);
            }
        }
    }

    public static class InvertedIndexCombiner
            extends Reducer<Text, Text, Text, Text>{
        private Text info = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            String positions = "";

            for(Text value : values){
                sum += 1;

                if (positions != "")
                    positions += ",";
                positions += value;
            }

            int splitIndex = key.toString().indexOf(":");

            String fullFileName = key.toString().substring(splitIndex+1);
            String[] fullFileNameSplited = fullFileName.split("/");
            String fileName = fullFileNameSplited[fullFileNameSplited.length - 1];
            String result = fileName + ":[count=" + sum + "; indexes={" + positions + "}]";

            info.set(result);
            key.set(key.toString().substring(0,splitIndex));
            context.write(key, info);
        }
    }

    public static class InvertedIndexReducer
            extends Reducer<Text, Text, Text, Text>{

        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {
            String fileList = new String();

            for(Text value : values){
                fileList += value.toString()+";";
            }
            result.set(fileList);
            context.write(key, result);
        }
    }

    public static void Start(String input, String output) throws IOException {
        Job job = new Job(new Configuration(), "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        try {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static <T> Collection<T> getCollectionFromIterable(Iterable<T> itr)
    {
        Collection<T> cltn = new ArrayList<T>();

        for (T t : itr)
            cltn.add(t);

        return cltn;
    }
}
