package net.talqum.hadoop.spark;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Created by Imre on 2014.11.25..
 */

public class TrafficLogEntry implements Comparable, Serializable{

    private static final long serialVersionUID = 7526472195622776147L;

    private String host;
    private LocalDateTime visitedAt;
    private String pageURL;

    public TrafficLogEntry(String logLine){
        String[] words = logLine.split(" ");

        this.host = words[0];
        this.pageURL = words[6];

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss").withLocale(Locale.US);
        this.visitedAt = LocalDateTime.parse(words[3].substring(1), formatter);
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

    @Override
    public int compareTo(Object o) {
        if (this == o) {
            return 0;
        }

        TrafficLogEntry that = (TrafficLogEntry) o;

        if(this.visitedAt.isAfter(that.visitedAt)){
            return -1;
        }
        else{
            return 1;
        }
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer();
        sb.append("{pageURL='").append(pageURL).append('\'');
        sb.append(", visitedAt=").append(visitedAt);
        sb.append('}');
        return sb.toString();
    }
}
