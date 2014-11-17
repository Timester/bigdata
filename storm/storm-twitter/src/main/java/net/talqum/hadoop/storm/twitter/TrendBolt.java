package net.talqum.hadoop.storm.twitter;

import java.util.HashMap;
import java.util.Map.Entry;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TrendBolt extends BaseBasicBolt {

    private final HashMap<String, Integer> elements = new HashMap<>();
    private static final long THRESHOLD = 30 * 1000;
    private long lastEmit = 0;

    private synchronized void increment(String element) {
        Integer count = elements.get(element);
        if (count == null) {
            elements.put(element, 1);
        } else {
            elements.put(element, count + 1);
        }
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String element = input.getStringByField("tweet-element");
        element = element.trim();
        increment(element);
        final long now = System.currentTimeMillis();
        if (now > lastEmit + THRESHOLD) {
            System.out.println("===========  EMITTING  ==========");
            for (Entry<String, Integer> entry : elements.entrySet()) {
                collector.emit(new Values(entry.getKey(), entry.getValue()));
            }
            elements.clear();
            System.out.println("=========  EMITTING END =========");
            lastEmit = now;
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("element", "elementcount"));
    }

}
