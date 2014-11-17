package net.talqum.hadoop.storm.twitter;

import twitter4j.Status;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HashtagExtractorBolt extends BaseBasicBolt {

    @Override
    public void cleanup() {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Status status = (Status) input.getValueByField("tweet");
        String[] text = status.getText().split(" ");
        for (String s : text) {
            if (s.startsWith("#")){
                collector.emit(new Values(s));
            }
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet-element"));
    }

}
