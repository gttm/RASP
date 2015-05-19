package gr.gttm;

import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import storm.kafka.trident.TridentKafkaState;

public class KafkaTopology {

	public static class SplitFields extends BaseBasicBolt {

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String line = tuple.getString(0);
			String[] words = line.split(" ", 2);
			if (words.length == 2) 				
				collector.emit(new Values(words[0], words[1]));	
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("key", "message"));
		}
	}

	public static void main(String[] args) throws Exception {
		// Spout configuration
		BrokerHosts brokerHosts = new ZkHosts("master:2181");
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "test", "/test",
				"storm");
		// Convert byte[] from kafka to string storm tuples
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		// Bolt configuration, FieldNameBasedTupleToKafkaMapper looks for fields
		// key, message by default
		KafkaBolt bolt = new KafkaBolt()
				.withTopicSelector(new DefaultTopicSelector("test1"))
				.withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("topicLine", new KafkaSpout(kafkaConfig), 1);
		builder.setBolt("keyMessage", new SplitFields(), 2).shuffleGrouping(
				"topicLine");
		builder.setBolt("forwardToKafka", bolt, 1)
				.shuffleGrouping("keyMessage");

		Config conf = new Config();
		// Set producer properties
		Properties props = new Properties();
		props.put("metadata.broker.list", "master:9092");
		props.put("request.required.acks", "1");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);

		conf.setDebug(true);
		conf.setNumWorkers(2);
		conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

		StormSubmitter.submitTopology("KafkaTopology", conf,
				builder.createTopology());
	}
}