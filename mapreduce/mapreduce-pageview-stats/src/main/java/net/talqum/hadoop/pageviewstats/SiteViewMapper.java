package net.talqum.hadoop.pageviewstats;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * HTML lapletöltések hisztogramja
 *  mapper -> host,1
 * Created by Imre on 2014.11.12..
 */
public class SiteViewMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokenized = line.split(" ");

        if(tokenized.length == 10){
            context.write(new Text(tokenized[0]), one);
        }
    }
}