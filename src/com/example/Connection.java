package com.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ProtocolOptions.Compression;

final public class Connection {
	
	static private Cluster cluster = null;
	static private Session session = null;
	
	static Session connect() {
		String contactPoint = "localhost";
		String keySpace = "ks1";
		
		if(session == null) {
			PoolingOptions poolingOptions = new PoolingOptions().setConnectionsPerHost(HostDistance.REMOTE, 1, 4);

			cluster = Cluster.builder().addContactPoint(contactPoint).withPoolingOptions(poolingOptions)
					.withCompression(Compression.SNAPPY).build();
			cluster.init();
			for (Host host : cluster.getMetadata().getAllHosts()) {
				System.out.printf("Address: %s, Rack: %s, Datacenter: %s, Tokens: %s\n", host.getAddress(),
						host.getDatacenter(), host.getRack(), host.getTokens());
			}
			session = cluster.connect(keySpace);
		}
		return session;
	}
	private Connection(){

    }
	
	static void close() {
		cluster.close();
	}
	

}
