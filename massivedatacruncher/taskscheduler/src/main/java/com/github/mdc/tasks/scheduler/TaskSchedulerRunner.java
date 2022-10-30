package com.github.mdc.tasks.scheduler;

import java.io.DataInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mdc.common.HeartBeat;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.ServerUtils;
import com.github.mdc.common.TaskSchedulerWebServlet;
import com.github.mdc.common.Utils;
import com.github.mdc.common.WebResourcesServlet;
import com.github.mdc.common.ZookeeperOperations;

public class TaskSchedulerRunner {

	static Logger log = LoggerFactory.getLogger(TaskSchedulerRunner.class);

	@SuppressWarnings({ "unused", "unchecked" })
	public static void main(String[] args) throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, MDCConstants.MDC_PROPERTIES);
		var cdl = new CountDownLatch(1);

		var hbs = new HeartBeat();
		hbs.init(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_RESCHEDULEDELAY)),
				Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT)),
				NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HOST)),
				Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_INITIALDELAY)),
				Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PINGDELAY)), "");
		hbs.start();
		var su = new ServerUtils();
		su.init(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_WEB_PORT)),
				new TaskSchedulerWebServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX,
				new WebResourcesServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.RESOURCES
						+ MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX);
		su.start();
		var es = Executors.newWorkStealingPool();
		var essingle = Executors.newSingleThreadExecutor();
		var cf = CuratorFrameworkFactory.newClient(
				MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_HOSTPORT), 20000, 50000, new RetryForever(
						Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_RETRYDELAY))));
			cf.start();
			cf.blockUntilConnected();
		ZookeeperOperations.addconnectionstate.addConnectionStateListener(cf,
				(CuratorFramework cfclient, ConnectionState cs) -> {
					if (cs == ConnectionState.RECONNECTED) {
						var nodedata = NetworkUtil
								.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HOST))
								+ MDCConstants.UNDERSCORE
								+ MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT);
						var nodesdata = (List<String>) ZookeeperOperations.nodesdata.invoke(cf,
								MDCConstants.ZK_BASE_PATH + MDCConstants.FORWARD_SLASH + MDCConstants.TASKSCHEDULER,
								null, null);
						if (!nodesdata.contains(nodedata))
							ZookeeperOperations.ephemeralSequentialCreate.invoke(cfclient,
									MDCConstants.ZK_BASE_PATH + MDCConstants.FORWARD_SLASH
											+ MDCConstants.TASKSCHEDULER,
									MDCConstants.TS + MDCConstants.HYPHEN, nodedata);
					}
				});
		ZookeeperOperations.ephemeralSequentialCreate.invoke(cf,
				MDCConstants.ZK_BASE_PATH + MDCConstants.FORWARD_SLASH + MDCConstants.TASKSCHEDULER,
				MDCConstants.TS + MDCConstants.HYPHEN,
				NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HOST))
						+ MDCConstants.UNDERSCORE + MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT));

		var ss = Utils.createSSLServerSocket(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT)));
		essingle.execute(() -> {
			while (true) {
				try {
					var s = ss.accept();
					var bytesl = new ArrayList<byte[]>();
					
					var in = new DataInputStream(s.getInputStream());
					var config = Utils.getConfigForSerialization();
					while (true) {
						var len = in.readInt();
						byte buffer[] = new byte[len]; // this could be reused !
						while (len > 0)
						    len -= in.read(buffer, buffer.length - len, len);
						// skipped: check for stream close
						Object obj = config.getObjectInput(buffer).readObject();
						if (obj instanceof Integer brkintval && brkintval == -1)
							break;
						bytesl.add((byte[]) obj);
					}
					String[] arguments = null;
					if (bytesl.size() > 2) {
						var totalargs = bytesl.size();
						arguments = new String[totalargs - 1];
						for (var index = 2; index < totalargs; index++) {
							arguments[index - 2] = new String(bytesl.get(index));
						}
					}
						es.execute(new TaskScheduler(cf, bytesl.get(0), arguments, s, new String(bytesl.get(1))));
					
				} catch (Exception ex) {
					log.error(MDCConstants.EMPTY, ex);
				}
			}
		});
		Utils.addShutdownHook(() -> {
			try {
				hbs.stop();
				hbs.destroy();
				log.debug("Destroying...");
				cf.close();
				es.shutdown();
				essingle.shutdown();
				su.stop();
				su.destroy();
				cdl.countDown();
				log.debug("Halting...");
			} catch (Exception e) {
				log.error(MDCConstants.EMPTY, e);
			}
		});
		String mrport = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT);
		String mrwebport = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_WEB_PORT);
		log.info("MapReduce scheduler kickoff at the ports[port={},webport={}]", mrport, mrwebport);
		cdl.await();
	}

}
