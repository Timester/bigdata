package net.talqum.hadoop.pageviewstats;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import static org.mockito.Mockito.*;

public class SiteViewMapperTest {
    @Test
    public void testMap() throws Exception {
        SiteViewMapper mapper = new SiteViewMapper();
        Text value = new Text("slip005.hol.nl - - [28/Jul/1995:13:32:01 -0400] \"GET /images/p263_100.jpg HTTP/1.0\" 200 49152");
        SiteViewMapper.Context context =  mock(SiteViewMapper.Context.class);
        mapper.map(null, value, context);

        verify(context).write(new Text("slip005.hol.nl"), new IntWritable(1));
    }
}