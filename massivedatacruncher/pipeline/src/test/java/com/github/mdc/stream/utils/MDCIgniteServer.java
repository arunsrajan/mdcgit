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
package com.github.mdc.stream.utils;


import static java.util.Objects.isNull;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;

public class MDCIgniteServer {

	private static Ignite ignite;

	private MDCIgniteServer() {
	}

	public static synchronized Ignite instance() {
		if (isNull(ignite)) {
			Ignition.setClientMode(false);
			var cfg = new IgniteConfiguration();
			cfg.setIgniteInstanceName("Server");
			// The node will be started as a server node.
			cfg.setClientMode(false);
			cfg.setDeploymentMode(DeploymentMode.CONTINUOUS);
			// Classes of custom Java logic will be transferred over the wire from
			// this app.
			cfg.setPeerClassLoadingEnabled(true);
			// Setting up an IP Finder to ensure the client can locate the servers.
			var ipFinder = new TcpDiscoveryMulticastIpFinder();
			ipFinder.setMulticastGroup(MDCProperties.get().getProperty(MDCConstants.IGNITEMULTICASTGROUP));
			cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));
			var cc = new CacheConfiguration<Object, byte[]>(MDCConstants.MDCCACHE);
			cc.setCacheMode(CacheMode.PARTITIONED);
			cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
			cc.setBackups(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.IGNITEBACKUP)));
			cfg.setCacheConfiguration(cc);
			ignite = Ignition.start(cfg);
		}
		return ignite;
	}

}
