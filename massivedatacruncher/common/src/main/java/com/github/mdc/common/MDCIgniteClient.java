package com.github.mdc.common;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

public class MDCIgniteClient {
	
	private MDCIgniteClient() {}
	
	private static Ignite ignite = null;
	
	public synchronized static Ignite instance(PipelineConfig pipelineconfig) {
		if(isNull(ignite) || nonNull(ignite) && !ignite.active()) {
			var cfg = new IgniteConfiguration();
			TcpCommunicationSpi commspi = new TcpCommunicationSpi();
			commspi.setMessageQueueLimit(20);
			commspi.setSlowClientQueueLimit(10);
			cfg.setCommunicationSpi(commspi);
			// The node will be started as a client node.
			cfg.setClientMode(true);
			cfg.setDeploymentMode(DeploymentMode.CONTINUOUS);
			// Classes of custom Java logic will be transferred over the wire from
			// this app.
			cfg.setPeerClassLoadingEnabled(true);
			// Setting up an IP Finder to ensure the client can locate the servers.
			var ipFinder = new TcpDiscoveryMulticastIpFinder();
			ipFinder.setMulticastGroup(pipelineconfig.getIgnitemulticastgroup());
			cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));
			var cc = new CacheConfiguration(MDCConstants.MDCCACHE);
			cc.setCacheMode(CacheMode.PARTITIONED);
			cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
			cc.setBackups(Integer.parseInt(pipelineconfig.getIgnitebackup()));
			cfg.setCacheConfiguration(cc);
			ignite = Ignition.start(cfg);
		}
		return ignite;
	}

}
