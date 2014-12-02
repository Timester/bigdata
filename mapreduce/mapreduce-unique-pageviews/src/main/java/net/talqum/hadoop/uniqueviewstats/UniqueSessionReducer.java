package net.talqum.hadoop.uniqueviewstats;

import com.google.common.collect.Sets;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer for UniqueSession counter
 * Created by Imre on 2014.11.12..
 */
public class UniqueSessionReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {

        context.write(key, new IntWritable(Sets.newHashSet(values).size()));
    }
}
