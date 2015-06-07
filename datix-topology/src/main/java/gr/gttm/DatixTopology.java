package gr.gttm;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;

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

import gr.gttm.bolt.IPToASBolt;
import gr.gttm.bolt.IPToDNSBolt;
import gr.gttm.bolt.SplitFieldsBolt;

public class DatixTopology {

	public static void main(String[] args) throws Exception {
		// Input from kafka and fields preprocessing
		BrokerHosts brokerHosts = new ZkHosts("zookeeper:2181");
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "netdata",
				"/netdata", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("netDataLine", new KafkaSpout(kafkaConfig), 2);
		builder.setBolt("netDataFields", new SplitFieldsBolt(), 2)
				.shuffleGrouping("netDataLine");
		
		// IP to AS
		builder.setBolt("ipToAS", new IPToASBolt(), 2)
				.shuffleGrouping("netDataFields");
		
		// IP to DNS
		builder.setBolt("ipToDNS", new IPToDNSBolt(), 6)
				.shuffleGrouping("ipToAS");
		
		// Output to hbase
		Map<String, Object> hbConf = new HashMap<String, Object>();
		hbConf.put("hbase.rootdir", "hdfs://master:9000/hbase");
		hbConf.put("hbase.zookeeper.quorum", "zookeeper");

		SimpleHBaseMapper mapper = new SimpleHBaseMapper()
				.withRowKeyField("dateTime")
				.withColumnFields(
						new Fields("sourceIP", "sourceIPInt", "destinationIP",
								"destinationIPInt", "protocol", "sourcePort",
								"destinationPort", "ipSize", "sourceAS",
								"destinationAS", "sourceDNS", "destinationDNS"))
				.withColumnFamily("cf");

		HBaseBolt hbaseBolt = new HBaseBolt("output", mapper)
				.withConfigKey("hbase.conf");
		builder.setBolt("output", hbaseBolt, 6).shuffleGrouping("ipToDNS");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(4);
		conf.setNumAckers(4);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 2000);
		conf.put("hbase.conf", hbConf);

		StormSubmitter.submitTopology("DatixTopology", conf,
				builder.createTopology());
	}
}