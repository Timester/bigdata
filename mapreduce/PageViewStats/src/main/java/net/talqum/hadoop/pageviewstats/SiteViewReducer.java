package net.talqum.hadoop.pageviewstats;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * reducer -> host, letöltésszám
 * Created by Imre on 2014.11.12..
 */
public class SiteViewReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable curr : values) {
            sum += curr.get();
        }

        context.write(key, new IntWritable(sum));
    }
}
