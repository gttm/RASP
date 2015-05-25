package gr.gttm;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.MoveFileAction;

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

public class NetDataTopology {
	private static final int TOP_N = 5;

	public static void main(String[] args) throws Exception {
		BrokerHosts brokerHosts = new ZkHosts("master:2181");
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "netdata",
				"/netdata", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("netDataLine", new KafkaSpout(kafkaConfig), 2);
		builder.setBolt("netDataFields", new SplitFieldsBolt(), 20)
				.shuffleGrouping("netDataLine");

		builder.setBolt("portCounter", new RollingCountBolt(30, 10), 20)
				.fieldsGrouping("netDataFields", "portStream", new Fields("port"));
		builder.setBolt("intermediatePortRanker",
				new IntermediateRankingsBolt(TOP_N), 10).fieldsGrouping(
				"portCounter", new Fields("obj"));
		builder.setBolt("topPorts", new TotalRankingsBolt(TOP_N, 10))
				.globalGrouping("intermediatePortRanker");

		SyncPolicy syncPolicy = new CountSyncPolicy(1);
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(10.0f,
				Units.MB);
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(
				"/storm/netdata/topPorts").withExtension(".txt");
		RecordFormat format = new DelimitedRecordFormat()
				.withFieldDelimiter(",");
		HdfsBolt portsHdfsBolt = new HdfsBolt()
				.withFsUrl("hdfs://master:9000")
				.withRotationPolicy(rotationPolicy)
				.withFileNameFormat(fileNameFormat)
				.withRecordFormat(format)
				.withSyncPolicy(syncPolicy)
				.addRotationAction(new MoveFileAction()
						.toDestination("/storm/netdata/topPorts/archive"));
		builder.setBolt("portsHdfs", portsHdfsBolt).shuffleGrouping("topPorts");

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(4);
		conf.setNumAckers(4);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);

		StormSubmitter.submitTopology("NetDataTopology", conf,
				builder.createTopology());
	}
}