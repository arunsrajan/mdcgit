/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

	private MDCIgniteClient() {
	}

	private static Ignite ignite;

	public synchronized static Ignite instance(PipelineConfig pipelineconfig) {
		if (isNull(ignite) || nonNull(ignite) && !ignite.active()) {
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
