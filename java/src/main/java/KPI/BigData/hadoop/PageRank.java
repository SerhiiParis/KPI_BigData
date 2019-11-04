package KPI.BigData.hadoop;

import KPI.BigData.hadoop.PageRankImpl.job1.WikiLinksReducer;
import KPI.BigData.hadoop.PageRankImpl.job1.WikiPageLinksMapper;
import KPI.BigData.hadoop.PageRankImpl.job1.XmlInputFormat;
import KPI.BigData.hadoop.PageRankImpl.job2.RankCalculateMapper;
import KPI.BigData.hadoop.PageRankImpl.job2.RankCalculateReduce;
import KPI.BigData.hadoop.PageRankImpl.job3.RankingMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class PageRank {
    public static void Start(String input, String output) throws IOException, InterruptedException, ClassNotFoundException {
        // runXmlParsing("inputs/pagerank_hadoop/xmlnodes", "outtt");

        NumberFormat nf = new DecimalFormat("00");

        String lastResultPath = null;
        String firstIteration = input;

        for (int runs = 0; runs < 5; runs++) {
            String inPath;

            if (runs == 0)
                inPath = firstIteration;
            else
                inPath = output + "iterations/" + nf.format(runs);

            lastResultPath = output + "iterations/" + nf.format(runs + 1);

            runRankCalculation(inPath, lastResultPath);
        }

        runRankOrdering(lastResultPath, output + "finish/");
    }

    public static boolean runXmlParsing(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        conf.set(XmlInputFormat.START_TAG_KEY, "<page>");
        conf.set(XmlInputFormat.END_TAG_KEY, "</page>");

        Job xmlHakker = Job.getInstance(conf, "xmlHakker");
        xmlHakker.setJarByClass(PageRank.class);

        // Input / Mapper
        FileInputFormat.addInputPath(xmlHakker, new Path(inputPath));
        xmlHakker.setInputFormatClass(XmlInputFormat.class);
        xmlHakker.setMapperClass(WikiPageLinksMapper.class);
        xmlHakker.setMapOutputKeyClass(Text.class);

        // Output / Reducer
        FileOutputFormat.setOutputPath(xmlHakker, new Path(outputPath));
        xmlHakker.setOutputFormatClass(TextOutputFormat.class);

        xmlHakker.setOutputKeyClass(Text.class);
        xmlHakker.setOutputValueClass(Text.class);
        xmlHakker.setReducerClass(WikiLinksReducer.class);

        return xmlHakker.waitForCompletion(true);
    }

    private static boolean runRankCalculation(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankCalculator = Job.getInstance(conf, "rankCalculator");
        rankCalculator.setJarByClass(PageRank.class);

        rankCalculator.setOutputKeyClass(Text.class);
        rankCalculator.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));

        rankCalculator.setMapperClass(RankCalculateMapper.class);
        rankCalculator.setReducerClass(RankCalculateReduce.class);

        return rankCalculator.waitForCompletion(true);
    }

    private static boolean runRankOrdering(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job rankOrdering = Job.getInstance(conf, "rankOrdering");
        rankOrdering.setJarByClass(PageRank.class);

        rankOrdering.setOutputKeyClass(FloatWritable.class);
        rankOrdering.setOutputValueClass(Text.class);

        rankOrdering.setMapperClass(RankingMapper.class);

        FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
        FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));

        rankOrdering.setInputFormatClass(TextInputFormat.class);
        rankOrdering.setOutputFormatClass(TextOutputFormat.class);

        return rankOrdering.waitForCompletion(true);
    }
}