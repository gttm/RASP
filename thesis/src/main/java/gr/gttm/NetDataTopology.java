package gr.gttm;

import java.util.Date;

import org.jnetpcap.packet.format.FormatUtils;

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

public class NetDataTopology {

	public static class SplitFields extends BaseBasicBolt {

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String line = tuple.getString(0);
			String[] fields = line.split(" ");

			collector.emit(new Values((Object[]) fields));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("sourceIp", "sourceIpInt",
					"destinationIp", "destinationIpInt", "protocol",
					"sourcePort", "destinationPort", "ipLength", "date"));
		}
	}

	public static void main(String[] args) throws Exception {
		BrokerHosts brokerHosts = new ZkHosts("master:2181");
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "netdata",
				"/netdata", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("netDataLine", new KafkaSpout(kafkaConfig), 1);
		builder.setBolt("netDataFields", new SplitFields(), 2).shuffleGrouping(
				"netDataLine");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(3);

		StormSubmitter.submitTopology("NetDataTopology", conf,
				builder.createTopology());
	}
}