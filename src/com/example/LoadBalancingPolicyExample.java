package com.example;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.ProtocolOptions.Compression;

public class LoadBalancingPolicyExample {
	
	static private Cluster cluster = null;
	static private Session session = null;
	
	static Session connect() {
		String contactPoint = "localhost";
		String keySpace = "ks1";
		
		if(session == null) {
			
			DCAwareRoundRobinPolicy dcAwarePolicy = new DCAwareRoundRobinPolicy.Builder().build();
			LoadBalancingPolicy policy = new TokenAwarePolicy(dcAwarePolicy);
			
			cluster = Cluster.builder().addContactPoint(contactPoint)
					.withLoadBalancingPolicy(policy).build();
			cluster.init();
			for (Host host : cluster.getMetadata().getAllHosts()) {
				System.out.printf("Address: %s, Rack: %s, Datacenter: %s, Tokens: %s\n", host.getAddress(),
						host.getDatacenter(), host.getRack(), host.getTokens());
			}
		}
		return session;
	}
	
	static void close() {
		cluster.close();
	}
	
	public static void main(String[] args) {
		connect();
		close();
	}
}
