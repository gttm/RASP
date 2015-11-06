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
		String[] fields = line.split(",");

		if (fields.length == 7) {
			String sourceIP = fields[0];
			long sourceIPInt = ipToInt(sourceIP);
			String destinationIP = fields[1];
			long destinationIPInt = ipToInt(destinationIP);
			short protocol = Short.parseShort(fields[2]);
			int sourcePort = Integer.parseInt(fields[3]);
			int destinationPort = Integer.parseInt(fields[4]);
			int ipSize = Integer.parseInt(fields[5]);
			long dateTime = Long.parseLong(fields[6]);

			collector.emit(new Values(sourceIP, sourceIPInt, destinationIP,
					destinationIPInt, protocol, sourcePort, destinationPort,
					ipSize, dateTime));
		}
	}
	
	private long ipToInt(String ipString) {
		String[] ipOctets = ipString.split("\\.");
		return Long.parseLong(ipOctets[0]) * 16777216
				+ Long.parseLong(ipOctets[1]) * 65536
				+ Long.parseLong(ipOctets[2]) * 256
				+ Long.parseLong(ipOctets[3]);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sourceIP", "sourceIPInt", "destinationIP",
				"destinationIPInt", "protocol", "sourcePort",
				"destinationPort", "ipSize", "dateTime"));
	}
}
