package gr.gttm;

import gr.gttm.bolt.IPToASBolt;
import gr.gttm.bolt.IPToDNSBolt;
import gr.gttm.bolt.SplitFieldsBolt;
import gr.gttm.tools.PhoenixConnectionProvider;
import gr.gttm.tools.PhoenixMapper;

import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

public class DatixTopology {

	public static void main(String[] args) throws Exception {
		String table = "netdata";
		String topic = "netdata";
		int parallelism[] = {4, 4, 4, 16, 36};
		int workers = 4;
		int pending = 100;
		int kafkaFetch = 128 * 1024;
		
		if (args.length > 0) {
			topic = args[0];
			table = args[1];
			String p[] = args[2].split(",");
			for (int i = 0; i < 5; i++)
				parallelism[i] = Integer.parseInt(p[i]);
			workers = Integer.parseInt(args[3]);
			pending = Integer.parseInt(args[4]);
			kafkaFetch = Integer.parseInt(args[5]) * 1024;
		}
		
		// Input from kafka and fields preprocessing
		BrokerHosts brokerHosts = new ZkHosts("zookeeper:2181", "/kafka/brokers");
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topic,
				"/" + topic, "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.bufferSizeBytes = 1024 * 1024;
		kafkaConfig.fetchSizeBytes = kafkaFetch;

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("netDataLine", new KafkaSpout(kafkaConfig), parallelism[0]);
		builder.setBolt("netDataFields", new SplitFieldsBolt(), parallelism[1])
				.shuffleGrouping("netDataLine");
		
		// IP to AS
		builder.setBolt("ipToAS", new IPToASBolt(), parallelism[2])
				.shuffleGrouping("netDataFields");
		
		// IP to DNS
		builder.setBolt("ipToDNS", new IPToDNSBolt(), parallelism[3])
				.shuffleGrouping("ipToAS");
		
		// Output to phoenix
		ConnectionProvider connectionProvider = new PhoenixConnectionProvider();
		JdbcMapper simpleJdbcMapper = new PhoenixMapper();
		JdbcInsertBolt phoenixBolt = new JdbcInsertBolt(
				connectionProvider, simpleJdbcMapper)
				.withInsertQuery("UPSERT INTO \"" + table + "\" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
				.withQueryTimeoutSecs(60);
		
		builder.setBolt("output", phoenixBolt, parallelism[4]).shuffleGrouping("ipToDNS");

		Config conf = new Config();
		conf.setNumWorkers(workers);
		conf.setNumAckers(workers);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, pending);

		StormSubmitter.submitTopology("DatixTopology", conf,
				builder.createTopology());
	}
}