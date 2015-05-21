package gr.gttm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import gr.gttm.bolt.SplitFieldsBolt;
import gr.gttm.bolt.IntermediateRankingsBolt;
import gr.gttm.bolt.RollingCountBolt;
import gr.gttm.bolt.TotalRankingsBolt;
import gr.gttm.tools.GetPortsBolt;

public class NetDataTopology {
	private static final int TOP_N = 5;

	public static void main(String[] args) throws Exception {
		BrokerHosts brokerHosts = new ZkHosts("master:2181");
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "netdata",
				"/netdata", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("netDataLine", new KafkaSpout(kafkaConfig), 2);
		builder.setBolt("netDataFields", new SplitFieldsBolt(), 2)
				.shuffleGrouping("netDataLine");

		builder.setBolt("port", new GetPortsBolt(), 2).shuffleGrouping(
				"netDataFields");
		builder.setBolt("portCounter", new RollingCountBolt(30, 10), 4)
				.fieldsGrouping("port", new Fields("port"));
		builder.setBolt("intermediatePortRanker",
				new IntermediateRankingsBolt(TOP_N), 4).fieldsGrouping(
				"portCounter", new Fields("obj"));
		builder.setBolt("topPorts", new TotalRankingsBolt(TOP_N, 10))
				.globalGrouping("intermediatePortRanker");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(8);

		StormSubmitter.submitTopology("NetDataTopology", conf,
				builder.createTopology());
	}
}