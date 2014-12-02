package net.talqum.hadoop.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.LocalDate;
import java.util.Map;

import scala.Tuple2;

public class SparkUniqueViewStats {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SparkUniqueViewStats <input-file> <output-folder>");
            System.exit(1);
        }

        final String outputPath = args[1];
        SparkConf sparkConf = new SparkConf().setAppName("SparkUniqueViewStats").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        // line example: uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] "GET / HTTP/1.0" 304 0
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        JavaRDD<TrafficLogEntry> entries = lines.map(s -> new TrafficLogEntry(s)).distinct();
        JavaPairRDD<LocalDate, TrafficLogEntry> trafficLogEntryJavaPairRDD =
            entries.mapToPair(entry -> new Tuple2<>(entry.getVisitedAt().toLocalDate(), entry));

        Map<LocalDate, Object> stringObjectMap = trafficLogEntryJavaPairRDD.countByKey();

        try(BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))){
            for (Map.Entry<LocalDate, Object> entry : stringObjectMap.entrySet()) {
                writer.write(entry.getKey() + "," + entry.getValue() + System.lineSeparator());
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        ctx.stop();
    }
}
