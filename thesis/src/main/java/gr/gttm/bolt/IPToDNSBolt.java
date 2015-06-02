package gr.gttm.bolt;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class IPToDNSBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 8081376680185858413L;
	private static final Logger LOG = Logger.getLogger(IPToASBolt.class);
	private Configuration config;
	private HTable table;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "zookeeper");
		try {
			table = new HTable(config, "rdns");
		} catch (IOException e) {
			LOG.error("Failed to connect to hbase");
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup() {
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String sourceIP = tuple.getStringByField("sourceIP");
		String destinationIP = tuple.getStringByField("destinationIP");
		List<Object> outputValues = tuple.getValues();
		outputValues.add(ipToDNS(sourceIP));
		outputValues.add(ipToDNS(destinationIP));

		collector.emit(new Values(outputValues.toArray()));
	}

	private String ipToDNS(String ip) {
		String dns = "null";
		Get g = new Get(Bytes.toBytes(ip));
		try {
			Result res = table.get(g);
			byte[] value = res.getValue(Bytes.toBytes("d"),
					Bytes.toBytes("dns"));
			dns = Bytes.toString(value);
		} catch (IOException e) {
			LOG.error("Failed to get row");
			e.printStackTrace();
		}
		return dns;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sourceIP", "sourceIPInt", "destinationIP",
				"destinationIPInt", "protocol", "sourcePort",
				"destinationPort", "ipSize", "dateTime", "sourceAS",
				"destinationAS", "sourceDNS", "destinationDNS"));
	}
}
