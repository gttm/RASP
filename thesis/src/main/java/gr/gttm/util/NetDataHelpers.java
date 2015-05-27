package gr.gttm.util;

public final class NetDataHelpers {
	
	private NetDataHelpers(){
	}
	
	public static long ipToInt(String ipString) {
		String[] ipOctets = ipString.split("\\.");
		return Long.parseLong(ipOctets[0]) * 16777216
				+ Long.parseLong(ipOctets[1]) * 65536
				+ Long.parseLong(ipOctets[2]) * 256
				+ Long.parseLong(ipOctets[3]);
		
	}
}
