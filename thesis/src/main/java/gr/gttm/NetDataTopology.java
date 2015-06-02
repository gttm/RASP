package gr.gttm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import gr.gttm.bolt.IPToASBolt;
import gr.gttm.bolt.SplitFieldsBolt;

public class NetDataTopology {

	public static void main(String[] args) throws Exception {
		// Input from Kafka and fields preprocessing
		BrokerHosts brokerHosts = new ZkHosts("zookeeper:2181");
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "netdata",
				"/netdata", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("netDataLine", new KafkaSpout(kafkaConfig), 2);
		builder.setBolt("netDataFields", new SplitFieldsBolt(), 30)
				.shuffleGrouping("netDataLine");
		
		// IP to AS
		builder.setBolt("ipToAS", new IPToASBolt(), 10)
				.shuffleGrouping("netDataFields");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(4);
		conf.setNumAckers(4);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);

		StormSubmitter.submitTopology("NetDataTopology", conf,
				builder.createTopology());
	}
}