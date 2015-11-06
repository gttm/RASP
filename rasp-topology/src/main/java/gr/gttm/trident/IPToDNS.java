package gr.gttm.trident;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class IPToDNS extends BaseFunction {
	private static final long serialVersionUID = 7264829738298533029L;
	private static final Logger LOG = Logger.getLogger(IPToDNS.class);
	private Configuration config;
	private HTable table;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TridentOperationContext context) {
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
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String sourceIP = tuple.getStringByField("sourceIP");
		String destinationIP = tuple.getStringByField("destinationIP");
		LOG.info(ipToDNS(sourceIP) + ", " + ipToDNS(destinationIP));
		collector.emit(new Values(ipToDNS(sourceIP), ipToDNS(destinationIP)));
	}

	private String ipToDNS(String ip) {
		String dns = "null";
		Get g = new Get(Bytes.toBytes(ip));
		try {
			Result res = table.get(g);
			byte[] value = res.getValue(Bytes.toBytes("d"),
					Bytes.toBytes("dns"));
			dns = Bytes.toString(value);
			if (dns == null)
				dns = "null";
		} catch (IOException e) {
			LOG.error("Failed to get row");
			e.printStackTrace();
		}
		return dns;
	}
}