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
package com.github.mdc.common.utils;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.Utils;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ServerCnxnFactory;

import java.util.concurrent.CountDownLatch;

public class ZookeeperMain {
	static Logger log = Logger.getLogger(ZookeeperMain.class);

	public static void main(String[] args) throws Exception {
		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, MDCConstants.MDC_PROPERTIES);
		var cdl = new CountDownLatch(1);
		var clientport = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_STANDALONE_CLIENTPORT,
				MDCConstants.ZOOKEEPER_STANDALONE_CLIENTPORT_DEFAULT));
		var numconnections = Integer
				.parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_STANDALONE_NUMCONNECTIONS,
						MDCConstants.ZOOKEEPER_STANDALONE_NUMCONNECTIONS_DEFAULT));
		var ticktime = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_STANDALONE_TICKTIME,
				MDCConstants.ZOOKEEPER_STANDALONE_TICKTIME_DEFAULT));
		try {
			ServerCnxnFactory scf = Utils.startZookeeperServer(clientport, numconnections, ticktime);
			Utils.addShutdownHook(() -> {
				cdl.countDown();
				log.info("Halting Zookeeper...");
				scf.closeAll();

			});
		} catch (Exception e) {
			log.error("Error in starting zookeeper", e);
		}
		cdl.await();
	}
}
