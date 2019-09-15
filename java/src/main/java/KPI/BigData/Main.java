package KPI.BigData;

public class Main {
    public static void main(String[] args) throws Exception {
        // MapReduce [hadoop]
        //KPI.BigData.WordCount.hadoop.WordCount.Start("inputs/mapreduce_hadoop.txt", "output/mapreduce_hadoop.txt");

        // MapReduce [spark]
        KPI.BigData.WordCount.spark.WordCount.Start("inputs/mapreduce_hadoop.txt", "output/mapreduce_hadoop.txt");
    }
}