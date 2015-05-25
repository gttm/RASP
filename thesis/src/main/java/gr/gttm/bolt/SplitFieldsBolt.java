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

		if (fields.length == 9) {
			// default stream
			collector.emit(new Values((Object[]) fields));
			// stream portStream
			collector.emit("portStream", new Values(fields[6]));
			collector.emit("portStream", new Values(fields[7]));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sourceIp", "sourceIpInt",
				"destinationIp", "destinationIpInt", "protocol", "sourcePort",
				"destinationPort", "ipSize", "date"));
		declarer.declareStream("portStream", new Fields("port"));
	}
}
