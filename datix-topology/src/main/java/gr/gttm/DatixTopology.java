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
		// Input from kafka and fields preprocessing
		BrokerHosts brokerHosts = new ZkHosts("zookeeper:2181", "/kafka/brokers");
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "netdata",
				"/netdata", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("netDataLine", new KafkaSpout(kafkaConfig), 4);
		builder.setBolt("netDataFields", new SplitFieldsBolt(), 4)
				.shuffleGrouping("netDataLine");
		
		// IP to AS
		builder.setBolt("ipToAS", new IPToASBolt(), 4)
				.shuffleGrouping("netDataFields");
		
		// IP to DNS
		builder.setBolt("ipToDNS", new IPToDNSBolt(), 6)
				.shuffleGrouping("ipToAS");
		
		// Output to phoenix
		ConnectionProvider connectionProvider = new PhoenixConnectionProvider();
		JdbcMapper simpleJdbcMapper = new PhoenixMapper();
		JdbcInsertBolt phoenixBolt = new JdbcInsertBolt(
				connectionProvider, simpleJdbcMapper)
				.withInsertQuery("UPSERT INTO \"netdata\" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
				.withQueryTimeoutSecs(60);
		
		builder.setBolt("output", phoenixBolt, 6).shuffleGrouping("ipToDNS");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(4);
		conf.setNumAckers(4);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 6000);
		//conf.put("hbase.conf", hbConf);

		StormSubmitter.submitTopology("DatixTopology", conf,
				builder.createTopology());
	}
}