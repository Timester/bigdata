package net.talqum.hadoop.spark;

import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import static org.junit.Assert.*;

public class TrafficLogEntryTest {

    @Test
    public void testConstructor() throws Exception {
        TrafficLogEntry entry = new TrafficLogEntry("uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] \"GET / HTTP/1.0\" 304 0");
    }

    @Test
    public void testDateParsing(){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss").withLocale(Locale.US);
        LocalDateTime visitedAt = LocalDateTime.parse("01/Aug/1995:00:00:07", formatter);

        assertEquals(1, visitedAt.getDayOfMonth());
        assertEquals(1995, visitedAt.getYear());
        assertEquals(0, visitedAt.getMinute());
        assertEquals(0, visitedAt.getHour());
        assertEquals(7, visitedAt.getSecond());
    }

    @Test
    public void testSubstring(){
        String in = "uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] \"GET / HTTP/1.0\" 304 0";

        assertEquals("01/Aug/1995", in.split(" ")[3].substring(1,12));
    }

    @Test
    public void testEquals(){
        TrafficLogEntry e2 = new TrafficLogEntry("uplherc.upl.com - - [01/Aug/1995:00:03:05 -0400] \"GET / HTTP/1.0\" 304 0");
        TrafficLogEntry e1 = new TrafficLogEntry("uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] \"GET / HTTP/1.0\" 304 0");

        assertEquals(e1, e1);
        assertEquals(e1, e2);

        e2 = new TrafficLogEntry("uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] \"GET / HTTP/1.0\" 304 0");
        e1 = new TrafficLogEntry("uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] \"GET / HTTP/1.0\" 304 0");

        assertEquals(e1, e2);

        e2 = new TrafficLogEntry("uplherc.upl.com - - [02/Aug/1995:00:00:07 -0400] \"GET / HTTP/1.0\" 304 0");
        e1 = new TrafficLogEntry("uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] \"GET / HTTP/1.0\" 304 0");

        assertNotEquals(e1, e2);

        e2 = new TrafficLogEntry("alma.upl.com - - [01/Aug/1995:00:00:07 -0400] \"GET / HTTP/1.0\" 304 0");
        e1 = new TrafficLogEntry("uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] \"GET / HTTP/1.0\" 304 0");

        assertNotEquals(e1, e2);

        e2 = new TrafficLogEntry("alma.upl.com - - [01/Aug/1995:00:00:07 -0400] \"GET / HTTP/1.0\" 304 0");
        e1 = new TrafficLogEntry("uplherc.upl.com - - [04/Aug/1995:00:00:07 -0400] \"GET / HTTP/1.0\" 304 0");

        assertNotEquals(e1, e2);

        e2 = new TrafficLogEntry("uplherc.upl.com - - [04/Jul/1995:00:00:07 -0400] \"GET / HTTP/1.0\" 304 0");
        e1 = new TrafficLogEntry("uplherc.upl.com - - [04/Aug/1995:00:00:07 -0400] \"GET / HTTP/1.0\" 304 0");

        assertNotEquals(e1, e2);
    }
}