package gr.gttm;

import gr.gttm.tools.PhoenixConnectionProvider;
import gr.gttm.tools.PhoenixMapper;

import java.util.Map;
import java.util.Random;

import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;

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

public class PhoenixTopology {
	
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
		//CREATE TABLE "phoenixtest" ("ip" VARCHAR PRIMARY KEY, "cf1"."dns" VARCHAR, "cf2"."latency" INTEGER);
		ConnectionProvider connectionProvider = new PhoenixConnectionProvider();
		JdbcMapper simpleJdbcMapper = new PhoenixMapper();	
		JdbcInsertBolt phoenixBolt = new JdbcInsertBolt(
				connectionProvider, simpleJdbcMapper)
				.withInsertQuery("UPSERT INTO \"phoenixtest\" VALUES (?,?,?)")
				.withQueryTimeoutSecs(30);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("row", new RandomRowSpout(), 1);
		builder.setBolt("output", phoenixBolt, 1).shuffleGrouping("row");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(4);

		StormSubmitter.submitTopology("PhoenixTopology", conf,
				builder.createTopology());
	}
}
