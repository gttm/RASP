package gr.gttm.bolt;

import gr.gttm.util.NetDataHelpers;

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
			long sourceIPInt = NetDataHelpers.ipToInt(sourceIP);
			String destinationIP = fields[1];
			long destinationIPInt = NetDataHelpers.ipToInt(destinationIP);
			String protocol = fields[2];
			String sourcePort = fields[3];
			String destinationPort = fields[4];
			String ipSize = fields[5];
			String dateTime = fields[6];

			collector.emit(new Values(sourceIP, sourceIPInt, destinationIP,
					destinationIPInt, protocol, sourcePort, destinationPort,
					ipSize, dateTime));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sourceIP", "sourceIPInt", "destinationIP",
				"destinationIPInt", "protocol", "sourcePort",
				"destinationPort", "ipSize", "dateTime"));
	}
}
