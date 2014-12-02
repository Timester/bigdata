package net.talqum.hadoop.pageviewstats;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SiteViewReducerTest {

    @Test
    public void testReduce() throws Exception {
        SiteViewReducer reducer = new SiteViewReducer();

        Text val = new Text("talqum.net");
        List<IntWritable> downloads = new ArrayList<>();
        downloads.add(new IntWritable(12));
        downloads.add(new IntWritable(8));
        downloads.add(new IntWritable(6));

        SiteViewReducer.Context ctx = mock(SiteViewReducer.Context.class);

        reducer.reduce(val, downloads, ctx);

        verify(ctx).write(val, new IntWritable(26));
    }
}