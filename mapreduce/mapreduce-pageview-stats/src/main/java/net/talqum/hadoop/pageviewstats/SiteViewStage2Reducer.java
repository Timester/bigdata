package net.talqum.hadoop.pageviewstats;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * letöltésszám, list<host>
 * reducer -> letöltésszám, hostszám
 * Created by Imre on 2014.11.12..
 */
public class SiteViewStage2Reducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        int cnt = 0;
        for (Text value : values) {
            cnt++;
        }

        context.write(key, new IntWritable(cnt));
    }
}
