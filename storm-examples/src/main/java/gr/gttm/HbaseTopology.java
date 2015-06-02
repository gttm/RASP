package gr.gttm;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class HbaseTopology {
	
	public static class RandomRowSpout extends BaseRichSpout {
		SpoutOutputCollector _collector;
		Random _rand;

		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			_collector = collector;
			_rand = new Random();
		}

		@Override
		public void nextTuple() {
			Utils.sleep(100);
			String randIp = _rand.nextInt(256) + "." + _rand.nextInt(256) + "."
					+ _rand.nextInt(256) + "." + _rand.nextInt(256);
			String[] dnsPool = new String[] { "cnn.com", "google.com",
					"twitter.com", "ntua.gr", "facebook.com" };
			String randDns = dnsPool[_rand.nextInt(dnsPool.length)];
			int randLatency = _rand.nextInt(1000);
			_collector.emit(new Values(randIp, randDns, randLatency));
		}

		@Override
		public void ack(Object id) {
		}

		@Override
		public void fail(Object id) {
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("ip", "dns", "latency"));
		}

	}

	public static void main(String[] args) throws Exception {
		Map<String, Object> hbConf = new HashMap<String, Object>();
        hbConf.put("hbase.rootdir", "hdfs://master:9000/hbase");
        hbConf.put("hbase.zookeeper.quorum", "zookeeper");
	    
		SimpleHBaseMapper mapper = new SimpleHBaseMapper()
				.withRowKeyField("ip")
				.withColumnFields(new Fields("dns", "latency"))
				.withColumnFamily("cf");

		HBaseBolt hbaseBolt = new HBaseBolt("test", mapper)
				.withConfigKey("hbase.conf");

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("row", new RandomRowSpout(), 1);
		builder.setBolt("output", hbaseBolt, 1).shuffleGrouping("row");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(4);
		conf.put("hbase.conf", hbConf);

		StormSubmitter.submitTopology("HbaseTopology", conf,
				builder.createTopology());
	}
}
