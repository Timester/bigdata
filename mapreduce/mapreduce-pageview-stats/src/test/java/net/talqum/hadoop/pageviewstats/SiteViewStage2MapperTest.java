package net.talqum.hadoop.pageviewstats;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.StringTokenizer;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class SiteViewStage2MapperTest {

    @Test
    public void testMap() throws Exception {
        SiteViewStage2Mapper mapper = new SiteViewStage2Mapper();
        Text value = new Text("alma\t12");
        SiteViewStage2Mapper.Context ctx = mock(SiteViewStage2Mapper.Context.class);

        mapper.map(new LongWritable(1), value, ctx);

        verify(ctx).write(new IntWritable(12), new Text("alma"));
    }

    @Test
    public void testTokenizer(){
        String a = "alma\t2\nkorte\t4\nbor\t6";

        StringTokenizer tokenizer = new StringTokenizer(a);

        assertEquals("alma", tokenizer.nextToken());
        assertEquals("2", tokenizer.nextToken());
        assertEquals("korte", tokenizer.nextToken());
        assertEquals("4", tokenizer.nextToken());

    }
}