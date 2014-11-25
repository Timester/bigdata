package net.talqum.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

import scala.Tuple2;

public class SparkWordCount {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SparkWordCount <input-file> <output-folder>");
            System.exit(1);
        }

        final String outputPath = args[1];
        SparkConf sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.replaceAll("[^A-Za-z 0-9]", "").toLowerCase().split("\\s+")));
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        counts.saveAsTextFile(outputPath);

        ctx.stop();
    }
}
