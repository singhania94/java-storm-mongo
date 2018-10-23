package org.github;

import java.util.Map;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.SimpleRecordTranslator;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class App extends BaseTopology {

	public static final String KAFKA_INPUT_TOPIC_NAMES = "kafka.input.topic.names";
	public static final String AUDIT_COLLECTION_NAME = "audit.collection.name";
	public static final String STREAM_SEPARATOR = ",";

	public static void main(String[] args) throws Exception {
		 App auditConsumerTopology = new App();
		 auditConsumerTopology.runTopology(args);

//		TopologyBuilder builder = new TopologyBuilder();
//		builder.setSpout("product_spout", new AggregateKafkaSpout(getKafkaConfig()), 3);
//		builder.setBolt("aggregate-bolt", new AggregateBolt(), 3).fieldsGrouping("product_spout", new Fields("key"));
//
//		Config conf = new Config();
//
//		Map<String, String> props = new PropertiesLoader().getProperties("local");
//
//		conf.put(GlobalProperties.TOPOLOGY_CONFIG, props);
//		conf.setDebug(false);
//		conf.setNumWorkers(1);
//		conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);
//		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
//		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 1024);
//		conf.put(Config.BACKPRESSURE_DISRUPTOR_HIGH_WATERMARK, 0.8);
//		conf.put(Config.BACKPRESSURE_DISRUPTOR_LOW_WATERMARK, 0.5);
//		conf.setMaxSpoutPending(1000);
//		conf.setMessageTimeoutSecs(60);
//
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("product_aggregation_topology", conf, builder.createTopology());
//
//		Thread.sleep(10000);
//		cluster.shutdown();
	}

	private static KafkaSpoutConfig<String, Integer> getKafkaConfig() {
		return KafkaSpoutConfig.builder("localhost:9092", "product").setKey(StringDeserializer.class)
				.setValue(IntegerDeserializer.class).setGroupId("product-consumer-group")
				.setRecordTranslator(new SimpleRecordTranslator<>(AggregateKafkaSpout.recordToTupleTranslator,
						new Fields(FieldNameBasedTupleToKafkaMapper.BOLT_KEY,
								FieldNameBasedTupleToKafkaMapper.BOLT_MESSAGE)))
				.build();
	}

	protected Config getTopologyConfig() {
		Config conf = new Config();

		Map<String, String> props = new PropertiesLoader().getProperties("local");
		conf.put(GlobalProperties.TOPOLOGY_CONFIG, props);

		conf.setDebug(false);
		conf.setNumWorkers(1);
		conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, true);
		conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 1024);
		conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 1024);
		conf.put(Config.BACKPRESSURE_DISRUPTOR_HIGH_WATERMARK, 0.8);
		conf.put(Config.BACKPRESSURE_DISRUPTOR_LOW_WATERMARK, 0.5);
		conf.setMaxSpoutPending(1000);
		conf.setMessageTimeoutSecs(60);
		return conf;
	}

	@Override
	public StormTopology buildTopology(Map<String, String> topologyConfig) throws Exception {
		final TopologyBuilder topologyBuilder = new TopologyBuilder();

		topologyBuilder.setSpout("product_spout", new AggregateKafkaSpout(getKafkaConfig()), 3);
		topologyBuilder.setBolt("agg_bolt", new AggregateBolt(), 3).fieldsGrouping("product_spout", new Fields("key"));

		return topologyBuilder.createTopology();
	}
}
