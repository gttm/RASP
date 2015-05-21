package gr.gttm.tools;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GetPortsBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1420483448811086816L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String sourcePort = tuple.getStringByField("sourcePort");
		String destinationPort = tuple.getStringByField("destinationPort");
		collector.emit(new Values(sourcePort));
		collector.emit(new Values(destinationPort));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("port"));
	}
}
