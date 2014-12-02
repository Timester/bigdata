package net.talqum.hadoop.pageviewstats;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SiteViewStage2ReducerTest {

    @Test
    public void testReduce() throws Exception {
        SiteViewStage2Reducer reducer = new SiteViewStage2Reducer();

        IntWritable val = new IntWritable(12);
        List<Text> hosts = new ArrayList<>();
        hosts.add(new Text("talqum.net"));
        hosts.add(new Text("index.hu"));
        hosts.add(new Text("aut.bme.hu"));

        SiteViewStage2Reducer.Context ctx = mock(SiteViewStage2Reducer.Context.class);

        reducer.reduce(val, hosts, ctx);

        verify(ctx).write(new IntWritable(12), new IntWritable(3));

    }
}