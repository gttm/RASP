package gr.gttm;

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

import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;

import java.util.Map;
import java.util.Random;

public class HiveTopology {

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
			int[] intPool = { 0, 1, 2, 3, 4 };
			String[] stringPool = new String[] { "string0", "string1",
					"string2", "string3", "string4" };
			int randInt = intPool[_rand.nextInt(intPool.length)];
			String randString = stringPool[_rand.nextInt(stringPool.length)];
			_collector.emit(new Values(randInt, randString));
		}

		@Override
		public void ack(Object id) {
		}

		@Override
		public void fail(Object id) {
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("col1", "col2"));
		}

	}

	public static void main(String[] args) throws Exception {
		String metaStoreURI = "thrift://master:9083";
		String dbName = "default";
		String tblName = "storm_test";
		String[] colNames = { "col1", "col2" };

		DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper()
				.withColumnFields(new Fields(colNames));

		HiveOptions hiveOptions = new HiveOptions(metaStoreURI, dbName,
				tblName, mapper).withTxnsPerBatch(10).withBatchSize(100)
				.withIdleTimeout(10);

		HiveBolt hiveBolt = new HiveBolt(hiveOptions);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("row", new RandomRowSpout(), 2);
		builder.setBolt("output", hiveBolt, 1).shuffleGrouping("row");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);

		StormSubmitter.submitTopology("HiveTopology", conf,
				builder.createTopology());
	}
}
