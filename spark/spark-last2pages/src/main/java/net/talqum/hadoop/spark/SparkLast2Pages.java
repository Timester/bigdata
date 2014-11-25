package net.talqum.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkLast2Pages {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SparkLast2Pages <input-file> <output-folder>");
            System.exit(1);
        }

        final String outputPath = args[1];
        SparkConf sparkConf = new SparkConf().setAppName("SparkLast2Pages").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        // line example: uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] "GET / HTTP/1.0" 304 0
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        JavaPairRDD<String, TrafficLogEntry> ones = lines.mapToPair(s -> new Tuple2<>(s.split(" ")[0], new TrafficLogEntry(s)));


        //TODO: finish this

        ctx.stop();
    }
}
