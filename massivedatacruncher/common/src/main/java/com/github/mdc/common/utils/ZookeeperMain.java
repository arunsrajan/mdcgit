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
		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.BACKWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.BACKWARD_SLASH, MDCConstants.MDC_PROPERTIES);
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
