package gr.gttm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.MoveFileAction;

import java.util.Map;

public class HdfsFileTopology {

	public static class ExclamationBolt extends BaseRichBolt {
		OutputCollector _collector;

		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void execute(Tuple tuple) {
			_collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
			_collector.ack(tuple);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}

	public static void main(String[] args) throws Exception {
		// sync the filesystem after every 1k tuples
		SyncPolicy syncPolicy = new CountSyncPolicy(1000);

		// rotate files when they reach 5MB
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(100.0f, Units.KB);

		FileNameFormat fileNameFormat = new DefaultFileNameFormat()
			.withPath("/storm/test/")
			.withExtension(".txt");

		// use "|" instead of "," for field delimiter
		RecordFormat format = new DelimitedRecordFormat()
			.withFieldDelimiter("|");

		// Instantiate the HdfsBolt
		HdfsBolt bolt = new HdfsBolt()
			.withFsUrl("hdfs://master:9000")
			.withFileNameFormat(fileNameFormat)
			.withRecordFormat(format)
			.withRotationPolicy(rotationPolicy)
			.withSyncPolicy(syncPolicy)
			.addRotationAction(new MoveFileAction().toDestination("/storm/test/archive/"));
		
		TopologyBuilder builder = new TopologyBuilder();

	    builder.setSpout("word", new TestWordSpout(), 2);
	    builder.setBolt("exclaim1", new ExclamationBolt(), 2).shuffleGrouping("word");
	    builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");
	    builder.setBolt("output", bolt, 2).shuffleGrouping("exclaim2");

	    Config conf = new Config();
	    conf.setDebug(true);

	    if (args != null && args.length > 0) {
	      conf.setNumWorkers(3);

	      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	    }
	}
}
