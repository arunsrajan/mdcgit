package com.github.mdc.tasks.scheduler;

import java.net.InetAddress;
import java.net.ServerSocket;
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
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.io.Input;
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

	static Logger log = Logger.getLogger(TaskSchedulerRunner.class);

	@SuppressWarnings({ "unused", "unchecked" })
	public static void main(String[] args) throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, MDCConstants.MDC_PROPERTIES);
		var cdl = new CountDownLatch(1);
		try (var cf = CuratorFrameworkFactory.newClient(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_HOSTPORT),
				20000, 50000,
				new RetryForever(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_RETRYDELAY))));
				var ss = Utils.createSSLServerSocket(
						Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT)));) {
			cf.start();
			var hbs = new HeartBeat();
			hbs.init(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_RESCHEDULEDELAY)),
					Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT)),
					NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HOST)),
					Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_INITIALDELAY)),
					Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PINGDELAY)),"");
			hbs.start();
			var su = new ServerUtils();
			su.init(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_WEB_PORT)),
					new TaskSchedulerWebServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX,
					new WebResourcesServlet(),
					MDCConstants.FORWARD_SLASH +MDCConstants.RESOURCES+MDCConstants.FORWARD_SLASH+ MDCConstants.ASTERIX);
			su.start();
			var es = Executors.newWorkStealingPool();
			var essingle = Executors.newFixedThreadPool(1);

			ZookeeperOperations.addconnectionstate.addConnectionStateListener(cf,
					(CuratorFramework cfclient, ConnectionState cs) -> {
						if (cs == ConnectionState.RECONNECTED) {
							var nodedata = NetworkUtil
									.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HOST))
									+ MDCConstants.UNDERSCORE + MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT);
							var nodesdata = (List<String>) ZookeeperOperations.nodesdata.invoke(cf,
									MDCConstants.ZK_BASE_PATH + MDCConstants.FORWARD_SLASH
											+ MDCConstants.TASKSCHEDULER,
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

			boolean ishdfs = Boolean.parseBoolean(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_ISHDFS));

			essingle.execute(() -> {
				while (true) {
					try {
						var s = ss.accept();
						var is = s.getInputStream();
						var os = s.getOutputStream();
						var baoss = new ArrayList<byte[]>();
						var kryo = Utils.getKryoSerializerDeserializer();
						var input = new Input(s.getInputStream());
						while (true) {
							var obj = kryo.readClassAndObject(input);
							log.debug("Input Object: " + obj);
							if (obj instanceof Integer brkval && brkval == -1)
								break;
							baoss.add((byte[]) obj);
						}
						if (ishdfs) {
							var mrjar = baoss.remove(0);
							var filename = baoss.remove(0);
							String[] argues = null;
							if (baoss.size() > 0) {
								var argsl = new ArrayList<>();
								for (var arg : baoss) {
									argsl.add(new String(arg));
								}
								argues = argsl.toArray(new String[argsl.size()]);
							}
							es.execute(new TaskScheduler(cf, mrjar, argues, s, new String(filename)));
						}
					} catch (Exception ex) {
						log.error(MDCConstants.EMPTY, ex);
					}
				}
			});
			log.info("MassiveDataTaskSchedulerDeamon started at port....."
					+ MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT));
			log.info("Adding Shutdown Hook...");
			Utils.addShutdownHook(() -> {
				try {
					log.debug("Stopping and closes all the connections...");
					hbs.stop();
					hbs.destroy();
					log.debug("Destroying...");
					es.shutdown();
					essingle.shutdown();
					su.stop();
					su.destroy();
					cdl.countDown();
					log.debug("Halting...");
					Runtime.getRuntime().halt(0);
				} catch (Exception e) {
					log.error(MDCConstants.EMPTY, e);
				}
			});
			cdl.await();
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
		    // Restore interrupted state...
		    Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.error(MDCConstants.EMPTY, ex);
		}
	}

}
