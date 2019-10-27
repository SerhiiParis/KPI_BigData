package KPI.BigData.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class WordCount {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void Start(String input, String __output) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .appName("wordcount_spark")
                .master("local[2]")
                .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
                .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(input).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        JavaPairRDD<Integer, String> swapped = counts.mapToPair(Tuple2::swap);
        swapped = swapped.sortByKey();

        List<Tuple2<String, Integer>> output = swapped.mapToPair(Tuple2::swap).collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        spark.stop();
    }
}