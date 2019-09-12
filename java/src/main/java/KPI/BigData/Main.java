package KPI.BigData;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.FileOutputFormat;

public class Main {
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(WordCount.Map.class);
        conf.setCombinerClass(WordCount.Reduce.class);
        conf.setReducerClass(WordCount.Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path("input_mapreduce.txt"));
        FileOutputFormat.setOutputPath(conf, new Path("output_mapreduce.txt"));

        JobClient.runJob(conf);
    }
}