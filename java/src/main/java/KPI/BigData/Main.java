package KPI.BigData;

import KPI.BigData.WordCount.hadoop.WordCount;

public class Main {
    public static void main(String[] args) throws Exception {
        WordCount.Worker.Start("inputs/mapreduce_hadoop.txt", "output/mapreduce_hadoop.txt");
    }
}