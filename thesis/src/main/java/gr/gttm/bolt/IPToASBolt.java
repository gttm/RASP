package gr.gttm.bolt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class IPToASBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -1585582852383812698L;
	private static final Logger LOG = Logger.getLogger(IPToASBolt.class);
	private static final String HDFS_URI = "hdfs://master:9000";
	private static final String IP_TO_AS_FILE = "/datasets/GeoIPASNum2.csv";
	private TreeMap<Long, String[]> asMap;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		asMap = new TreeMap<Long, String[]>();
		Configuration configuration = new Configuration();
		Path fp = new Path(IP_TO_AS_FILE);
		configuration.addResource(fp);
		while (asMap.isEmpty()){
			try {
				FileSystem fs = FileSystem.get(new URI(HDFS_URI), configuration);
				BufferedReader lineReader = new BufferedReader(
						new InputStreamReader(fs.open(fp)));

				String line = null;
				while ((line = lineReader.readLine()) != null) {
					String[] fields = line.split(",");
					long ipIntStart = Long.parseLong(fields[0]);
					long ipIntEnd = Long.parseLong(fields[1]);
					String as = fields[2].replaceAll("^\"|\"$", "");
					asMap.put(ipIntStart, new String[]{as, "start"});
					asMap.put(ipIntEnd, new String[]{as, "stop"});
				}
				lineReader.close();
			} catch (IOException | URISyntaxException e) {
				LOG.error("Failed to create the TreeMap");
				e.printStackTrace();
			}
		}
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		long sourceIPInt = tuple.getLongByField("sourceIPInt");
		long destinationIPInt = tuple.getLongByField("destinationIPInt");
		List<Object> outputValues = tuple.getValues();
		outputValues.add(ipToAS(sourceIPInt));
		outputValues.add(ipToAS(destinationIPInt));
		
		collector.emit(new Values(outputValues.toArray()));
	}
	
	private String ipToAS(long ipInt) {
		String as = "null";
		Long key = asMap.ceilingKey(ipInt);		
		if (key != null) {
			String[] value = asMap.get(key);
			// Determine if we are in the ip range of any of the provided ASes
			if ((key == ipInt) || (value[1].equals("stop")))
				as = value[0];
		}
		return as;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sourceIP", "sourceIPInt", "destinationIP",
				"destinationIPInt", "protocol", "sourcePort",
				"destinationPort", "ipSize", "dateTime", "sourceAS",
				"destinationAS"));
	}
}
