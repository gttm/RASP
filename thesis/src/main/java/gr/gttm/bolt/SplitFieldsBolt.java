package gr.gttm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitFieldsBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -1585582852383812698L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String line = tuple.getString(0);
		String[] fields = line.split(" ");

		if (fields.length == 7) {
			String sourceIp = fields[0];
			long sourceIpInt = ipToInt(sourceIp);
			String destinationIp = fields[1];
			long destinationIpInt = ipToInt(destinationIp);
			String protocol = fields[2];
			String sourcePort = fields[3];
			String destinationPort = fields[4];
			String ipSize = fields[5];
			String date = fields[6];
			
			// default stream
			collector.emit(new Values(sourceIp, sourceIpInt,
					destinationIp, destinationIpInt, protocol,
					sourcePort, destinationPort, ipSize, date));
			
			// stream portStream
			collector.emit("portStream", new Values(sourcePort));
			collector.emit("portStream", new Values(destinationPort));
		}
	}

	public long ipToInt(String ipString) {
		String[] ipOctets = ipString.split("\\.");
		return Long.parseLong(ipOctets[0]) * 16777216
				+ Long.parseLong(ipOctets[1]) * 65536
				+ Long.parseLong(ipOctets[2]) * 256
				+ Long.parseLong(ipOctets[3]);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sourceIp", "sourceIpInt", "destinationIp",
				"destinationIpInt", "protocol", "sourcePort",
				"destinationPort", "ipSize", "date"));
		declarer.declareStream("portStream", new Fields("port"));
	}
}
