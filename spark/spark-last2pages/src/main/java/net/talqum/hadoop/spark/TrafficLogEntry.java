package net.talqum.hadoop.spark;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created by Imre on 2014.11.25..
 */
public class TrafficLogEntry {
    private String host;
    private LocalDateTime visitedAt;
    private String pageURL;

    public TrafficLogEntry(String logLine){
        String[] words = logLine.split(" ");

        this.host = words[0];
        this.pageURL = words[6];

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss");
        this.visitedAt = LocalDateTime.parse(words[4].substring(1), formatter);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public LocalDateTime getVisitedAt() {
        return visitedAt;
    }

    public void setVisitedAt(LocalDateTime visitedAt) {
        this.visitedAt = visitedAt;
    }

    public String getPageURL() {
        return pageURL;
    }

    public void setPageURL(String pageURL) {
        this.pageURL = pageURL;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TrafficLogEntry that = (TrafficLogEntry) o;

        if (!host.equals(that.host)) {
            return false;
        }
        if (!pageURL.equals(that.pageURL)) {
            return false;
        }
        if (!visitedAt.equals(that.visitedAt)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + visitedAt.hashCode();
        result = 31 * result + pageURL.hashCode();
        return result;
    }
}
