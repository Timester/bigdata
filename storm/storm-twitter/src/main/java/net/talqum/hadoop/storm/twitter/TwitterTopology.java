package net.talqum.hadoop.storm.twitter;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TwitterTopology {

    private Properties properties = new Properties();

    public static void main(String[] args) {
        TwitterTopology twitterTopology = new TwitterTopology();
        twitterTopology.start();
    }

    public void start(){
        loadproperties();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweets-collector", new TwitterSpout(properties));
        builder.setBolt("urlemitter", new LinkExtractorBolt()).shuffleGrouping("tweets-collector");
        builder.setBolt("urlcounter", new TrendBolt()).fieldsGrouping("urlemitter", new Fields("tweet-link"));
        builder.setBolt("urlcounter-printer", new PritingBolt()).shuffleGrouping("urlcounter");
        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        cluster.submitTopology("twitter-test", conf, builder.createTopology());
    }

    private void loadproperties(){
        try(InputStream is = new FileInputStream("auth.properties")){
            properties.load(is);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
