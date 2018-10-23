package org.github;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class AggregateBolt extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	private Map<String, List<Integer>> count = new HashMap<String, List<Integer>>();

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

	}

	public void execute(Tuple input) {
		String key = input.getString(0);
		int value = input.getInteger(1);

		if(count.containsKey(key))
			count.get(key).add(value);
		else {
			count.put(key, new ArrayList<>());
			count.get(key).add(value);
		}

		System.out.println(count);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
