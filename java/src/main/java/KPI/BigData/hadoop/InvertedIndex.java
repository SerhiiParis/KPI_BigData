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
import java.util.StringTokenizer;

public class InvertedIndex {
    public static class InvertedIndexMapper extends
            Mapper<Object,Text,Object,Text>{
        private Text keyInfo = new Text();//store the combination of word and URI
        private  Text valueInfo = new Text();//store the word frequency
        private FileSplit split;//store the split target
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //there are 3 input files：1.txt("MapReduce is Simple")
            //2.txt("MapReduce is powerful is Simple")
            //3.txt("Hello MapReduce bye MapReduce")

            split = (FileSplit)context.getInputSplit();
            //get the FileSplit tatget of <key,value>

            //"value" saves one line of message in the txt file
            //"key" is the offset of the initial and the head address of txt file

            StringTokenizer itr = new StringTokenizer(value.toString());
            //StringTokenizer splits each row into words combination and output <word, 1> as the result of mapping

            while(itr.hasMoreTokens()){
                //key is the combination of words and URI
                keyInfo.set(itr.nextToken()+":"+split.getPath().toString());
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);
                //output：<key,value>---<"MapReduce:1.txt",1>
            }
        }
    }

    public static class InvertedIndexCombiner
            extends Reducer<Text, Text, Text, Text>{
        private Text info = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {
            //input：<key,value>---<"MapReduce:1.txt",list(1,1,1,1)>
            //key="MapReduce:1.txt",value=list(1,1,1,1);
            int sum = 0;
            for(Text value : values){
                sum += Integer.parseInt(value.toString());
            }

            int splitIndex = key.toString().indexOf(":");
            info.set(key.toString().substring(splitIndex+1)+":"+sum);
            key.set(key.toString().substring(0,splitIndex));
            context.write(key, info);
            //output:<key,value>----<"Mapreduce","0.txt:2">
        }
    }

    public static class InvertedIndexReducer
            extends Reducer<Text, Text, Text, Text>{

        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values,Context context)
                throws IOException, InterruptedException {
            //input：<"MapReduce",list("0.txt:1","1.txt:1","2.txt:1")>
            //output：<"MapReduce","0.txt:1,1.txt:1,2.txt:1">
            String fileList = new String();
            for(Text value : values){//value="0.txt:1"
                fileList += value.toString()+";";
            }
            result.set(fileList);
            context.write(key, result);
            //output：<"MapReduce","0.txt:1,1.txt:1,2.txt:1">
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
}
