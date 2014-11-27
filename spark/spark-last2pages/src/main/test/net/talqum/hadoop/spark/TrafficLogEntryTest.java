package net.talqum.hadoop.spark;

import org.junit.Test;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import static org.junit.Assert.assertEquals;

public class TrafficLogEntryTest {

    @Test
    public void testConstructor() throws Exception {
        TrafficLogEntry entry = new TrafficLogEntry("line example: uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] \"GET / HTTP/1.0\" 304 0");
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
}