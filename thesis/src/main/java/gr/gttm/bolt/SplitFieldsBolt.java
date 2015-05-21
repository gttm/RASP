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

		collector.emit(new Values((Object[]) fields));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sourceIp", "sourceIpInt", "destinationIp",
				"destinationIpInt", "protocol", "sourcePort",
				"destinationPort", "ipLength", "date"));
	}
}
