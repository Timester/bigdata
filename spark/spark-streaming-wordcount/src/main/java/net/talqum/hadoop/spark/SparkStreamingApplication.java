package net.talqum.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;
import java.util.Arrays;

import scala.Tuple2;

public class SparkStreamingApplication implements Runnable, Serializable {

    public static void main(String[] args) {
        SparkStreamingApplication sparkStream = new SparkStreamingApplication();
        sparkStream.run();
    }

    public void run() {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")));
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> result = pairs.reduceByKey((arg0, arg1) -> arg0 + arg1);

        result.print();

        jssc.start(); // Start the computation
        jssc.awaitTermination(); // Wait for the computation to terminate
    }
}
