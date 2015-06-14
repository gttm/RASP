package gr.gttm;

import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.trident.state.JdbcState;
import org.apache.storm.jdbc.trident.state.JdbcStateFactory;
import org.apache.storm.jdbc.trident.state.JdbcUpdater;

import gr.gttm.tools.PhoenixConnectionProvider;
import gr.gttm.tools.PhoenixMapper;
import gr.gttm.trident.IPToAS;
import gr.gttm.trident.IPToDNS;
import gr.gttm.trident.SplitFields;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;

import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;

public class DatixTridentTopology {

	public static void main(String[] args) throws Exception {
		// Kafka configuration
		BrokerHosts brokerHosts = new ZkHosts("zookeeper:2181",	"/kafka/brokers");
		TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokerHosts,
				"netdata", "storm-trident");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.bufferSizeBytes = 1024 * 1024;
		kafkaConfig.fetchSizeBytes = 10 * 1024;
		OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);

		// Phoenix configuration
		ConnectionProvider connectionProvider = new PhoenixConnectionProvider();
		JdbcMapper simpleJdbcMapper = new PhoenixMapper();
		JdbcState.Options options = new JdbcState.Options()
				// spelling mistake on storm-jdbc
				.withConnectionPrvoider(connectionProvider)
				.withMapper(simpleJdbcMapper)
				.withInsertQuery("UPSERT INTO \"netdata\" VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
				.withQueryTimeoutSecs(60);
		JdbcStateFactory jdbcStateFactory = new JdbcStateFactory(options);
		
		// Trident topology
		TridentTopology topology = new TridentTopology();
		topology.newStream("netDataStream", kafkaSpout)
				.parallelismHint(4)
				.shuffle()
				// OpaqueTridentKafkaSpout uses the field "str" to emit the messages 
				.each(new Fields("str"), new SplitFields(),
						new Fields("sourceIP", "sourceIPInt", "destinationIP",
								"destinationIPInt", "protocol", "sourcePort",
								"destinationPort", "ipSize", "dateTime"))
				.each(new Fields("sourceIPInt", "destinationIPInt"), new IPToAS(),
						new Fields("sourceAS", "destinationAS"))
				.each(new Fields("sourceIP", "destinationIP"), new IPToDNS(),
						new Fields("sourceDNS", "destinationDNS"))
				.partitionPersist(jdbcStateFactory,
						new Fields("sourceIP", "sourceIPInt", "destinationIP",
								"destinationIPInt", "protocol", "sourcePort",
								"destinationPort", "ipSize", "dateTime",
								"sourceAS", "destinationAS", "sourceDNS",
								"destinationDNS"), 
						new JdbcUpdater(), new Fields())
				.parallelismHint(8);

		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(4);
		conf.setNumAckers(4);
		conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 20);
		conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 200);

		StormSubmitter.submitTopology("DatixTridentTopology", conf,	topology.build());
	}
}
