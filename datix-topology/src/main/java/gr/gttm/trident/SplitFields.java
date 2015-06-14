package gr.gttm.trident;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class SplitFields extends BaseFunction {
	private static final long serialVersionUID = -8393875916129429538L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
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
}
