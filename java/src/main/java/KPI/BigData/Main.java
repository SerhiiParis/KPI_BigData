package KPI.BigData;

public class Main {
    public static void main(String[] args) throws Exception {
        // MapReduce [hadoop]
        KPI.BigData.hadoop.WordCount.Start("inputs/mapreduce_hadoop.txt", "output/mapreduce_hadoop.txt");
        //KPI.BigData.hadoop.InvertedIndex.Start("inputs/invertedindex_hadoop", "output/invertedindex_hadoop");

        // MapReduce [spark]
        //KPI.BigData.WordCount.spark.WordCount.Start("inputs/mapreduce_hadoop.txt", "output/mapreduce_hadoop.txt");
    }
}