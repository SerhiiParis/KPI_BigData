package KPI.BigData;

import KPI.BigData.WordCount.hadoop.WordCount;

public class Main {
    public static void main(String[] args) throws Exception {
        WordCount.Worker.Start("input_mapreduce.txt", "output_mapreduce.txt");
    }
}