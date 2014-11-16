package net.talqum.hadoop.uniqueviewstats;

import org.apache.hadoop.io.Text;
import org.junit.Test;
import static org.mockito.Mockito.*;

public class UniqueSessionMapperTest {

    @Test
    public void testMap() throws Exception {
        UniqueSessionMapper mapper = new UniqueSessionMapper();

        Text input = new Text("slip005.hol.nl - - [28/Jul/1995:13:32:01 -0400] \"GET /images/p263_100.jpg HTTP/1.0\" 200 49152");
        UniqueSessionMapper.Context cntx = mock(UniqueSessionMapper.Context.class);
        mapper.map(null, input, cntx);

        verify(cntx).write(new Text("28/Jul/1995"), new Text("slip005.hol.nl"));
    }

    @Test
    public void testMapWithBadInput() throws Exception {
        UniqueSessionMapper mapper = new UniqueSessionMapper();

        Text input = new Text("alma");
        UniqueSessionMapper.Context cntx = mock(UniqueSessionMapper.Context.class);
        mapper.map(null, input, cntx);

        verify(cntx, never()).write(new Text(""), new Text(""));
    }
}