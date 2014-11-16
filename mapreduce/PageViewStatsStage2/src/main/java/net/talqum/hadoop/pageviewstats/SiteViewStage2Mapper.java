package net.talqum.hadoop.pageviewstats;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * HTML lapletöltések hisztogramja
 * host, letöltésszám jön a stage 1 ből
 * Created by Imre on 2014.11.12..
 */
public class SiteViewStage2Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        Text host = new Text(tokenizer.nextToken());
        IntWritable views = new IntWritable(Integer.parseInt(tokenizer.nextToken()));

        context.write(views, host);
    }
}