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
        loadProperties();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("tweets-collector", new TwitterSpout(properties));

        builder.setBolt("urlemitter", new LinkExtractorBolt()).shuffleGrouping("tweets-collector");
        builder.setBolt("hashtagemitter", new HashtagExtractorBolt()).shuffleGrouping("tweets-collector");

        builder.setBolt("urlcounter", new TrendBolt()).fieldsGrouping("urlemitter", new Fields("tweet-element"));
        builder.setBolt("hashtagcounter", new TrendBolt()).fieldsGrouping("hashtagemitter", new Fields("tweet-element"));

       // builder.setBolt("urlcounter-printer", new PritingBolt()).shuffleGrouping("urlcounter");
      //  builder.setBolt("hashtagcounter-printer", new PritingBolt()).shuffleGrouping("hashtagcounter");
        builder.setBolt("counter-printer", new PritingBolt()).shuffleGrouping("urlcounter").shuffleGrouping("hashtagcounter");

        LocalCluster cluster = new LocalCluster();
        Config conf = new Config();
        cluster.submitTopology("twitter-test", conf, builder.createTopology());
    }

    private void loadProperties(){
        try(InputStream is = new FileInputStream("auth.properties")){
            properties.load(is);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
