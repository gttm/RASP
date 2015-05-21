package gr.gttm;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.jnetpcap.Pcap;
import org.jnetpcap.packet.JPacket;
import org.jnetpcap.packet.JPacketHandler;
import org.jnetpcap.protocol.network.Ip4;
import org.jnetpcap.protocol.tcpip.Tcp;
import org.jnetpcap.protocol.tcpip.Udp;
import org.jnetpcap.packet.format.FormatUtils;

public class PcapProducer {
	public static void main(String[] args) throws Exception {
		if (args == null || args.length != 2) {
			System.out.println("Usage: PcapProducer pcap-file delay-ms");
			return;
		}

		final String file = args[0];
		final int delay = Integer.parseInt(args[1]);
		StringBuilder errbuf = new StringBuilder();

		Properties props = new Properties();
		props.put("metadata.broker.list", "master:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// props.put("partitioner.class", "example.producer.SimplePartitioner");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		final Producer<String, String> producer = new Producer<String, String>(
				config);

		while (true) {
			Pcap pcap = Pcap.openOffline(file, errbuf);
			if (pcap == null) {
				System.err.println(errbuf);
				return;
			}

			pcap.loop(Pcap.LOOP_INFINITE, new JPacketHandler<StringBuilder>() {
				Ip4 ip4 = new Ip4();
				Tcp tcp = new Tcp();
				Udp udp = new Udp();

				public void nextPacket(JPacket packet, StringBuilder errbuf) {
					if (packet.hasHeader(ip4)
							&& (packet.hasHeader(Tcp.ID) || packet
									.hasHeader(Udp.ID))) {
						String sourceIp = FormatUtils.ip(ip4.source());
						// Convert unsigned int to long
						long sourceIpInt = (long) (ip4.sourceToInt() & 0xffffffffl);
						String destinationIp = FormatUtils.ip(ip4.destination());
						long destinationIpInt = (long) (ip4.destinationToInt() & 0xffffffffl);
						int protocol = ip4.type();
						int sourcePort = 0;
						int destinationPort = 0;
						int ipLength = ip4.length();
						long date = packet.getCaptureHeader().timestampInMillis();

						if (packet.hasHeader(tcp)) {
							sourcePort = tcp.source();
							destinationPort = tcp.destination();
						} else if (packet.hasHeader(udp)) {
							sourcePort = udp.source();
							destinationPort = udp.destination();
						}

						String message = String.format(
								"%s %d %s %d %d %d %d %d %d", sourceIp,
								sourceIpInt, destinationIp, destinationIpInt,
								protocol, sourcePort, destinationPort,
								ipLength, date);
						KeyedMessage<String, String> data = new KeyedMessage<String, String>(
								"netdata", message);
						producer.send(data);
						
						try {
							Thread.sleep(delay);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}, errbuf);

			pcap.close();
		}
	}
}