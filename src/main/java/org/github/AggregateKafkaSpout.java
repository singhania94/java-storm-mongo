package com.citi;

import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.tuple.Values;

public class AggregateKafkaSpout extends KafkaSpout<String, Integer> {


	private static final long serialVersionUID = 1L;

	public AggregateKafkaSpout(KafkaSpoutConfig<String, Integer> kafkaSpoutConfig) {
		super(kafkaSpoutConfig);
	}

	public static Func<ConsumerRecord<String, Integer>, List<Object>> recordToTupleTranslator = new Func<ConsumerRecord<String, Integer>, List<Object>>() {

		private static final long serialVersionUID = 1L;
		@Override
		public List<Object> apply(ConsumerRecord<String, Integer> r) {
			return new Values(r.key(), r.value());
		}
	};

}
