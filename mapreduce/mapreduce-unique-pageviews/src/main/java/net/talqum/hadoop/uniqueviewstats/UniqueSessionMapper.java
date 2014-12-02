package net.talqum.hadoop.uniqueviewstats;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Mapper for UniqueSession counter
 * Created by Imre on 2014.11.12..
 */
public class UniqueSessionMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokenized = line.split(" ");

        if(tokenized.length == 10){
            Text host = new Text(tokenized[0]);
            Text date = new Text(tokenized[3].substring(1, tokenized[3].indexOf(":")));

            context.write(date, host);
        }
    }
}
