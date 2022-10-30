package com.github.mdc.stream.scheduler;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.jgroups.JChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mdc.common.CacheUtils;
import com.github.mdc.common.HeartBeatStream;
import com.github.mdc.common.Job;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.ServerUtils;
import com.github.mdc.common.TaskSchedulerWebServlet;
import com.github.mdc.common.Utils;
import com.github.mdc.common.WebResourcesServlet;
import com.github.mdc.common.ZookeeperOperations;

/**
 * 
 * @author Arun The task scheduler daemon process.
 */
public class StreamPipelineTaskSchedulerRunner {
	static Logger log = LoggerFactory.getLogger(StreamPipelineTaskSchedulerRunner.class);
	private static HeartBeatStream hbss;
	static ServerSocket ss = null;
	static ExecutorService esstream;
	static ExecutorService es;
	static JChannel channel;
	static Map<String, Job> jobidjobmap = new ConcurrentHashMap<>();
	static ClassLoader cl;
	static ExecutorService threadpool = null;

	public static void main(String[] args) throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		threadpool = Executors.newWorkStealingPool();
		// Load log4j properties.
		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, MDCConstants.MDC_PROPERTIES);
		CacheUtils.initCache();
		var cdl = new CountDownLatch(1);


		var esstream = Executors.newFixedThreadPool(1);
		var es = Executors.newWorkStealingPool();
		var su = new ServerUtils();
		su.init(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_WEB_PORT)),
				new TaskSchedulerWebServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX,
				new WebResourcesServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.RESOURCES
						+ MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX);
		su.start();
		var cf = CuratorFrameworkFactory.newClient(
				MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_HOSTPORT), 20000, 50000, new RetryForever(
						Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_RETRYDELAY))));
			cf.start();
			cf.blockUntilConnected();
		if (!(boolean) ZookeeperOperations.checkexists.invoke(cf,
				MDCConstants.FORWARD_SLASH + MDCProperties.get().getProperty(MDCConstants.CLUSTERNAME)
						+ MDCConstants.FORWARD_SLASH + MDCConstants.TSS,
				MDCConstants.LEADER,
				NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST))
						+ MDCConstants.UNDERSCORE
						+ MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT))) {
			ZookeeperOperations.persistentCreate.invoke(cf,
					MDCConstants.FORWARD_SLASH + MDCProperties.get()
							.getProperty(MDCConstants.CLUSTERNAME) + MDCConstants.FORWARD_SLASH + MDCConstants.TSS,
					MDCConstants.LEADER,
					NetworkUtil
							.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST))
							+ MDCConstants.UNDERSCORE
							+ MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT));
		} else {
			ZookeeperOperations.writedata
					.invoke(cf,
							MDCConstants.FORWARD_SLASH
									+ MDCProperties.get()
											.getProperty(MDCConstants.CLUSTERNAME)
									+ MDCConstants.FORWARD_SLASH + MDCConstants.TSS + MDCConstants.FORWARD_SLASH
									+ MDCConstants.LEADER,
							MDCConstants.EMPTY,
							NetworkUtil.getNetworkAddress(
									MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST))
									+ MDCConstants.UNDERSCORE
									+ MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT));
		}
		var hbss = new HeartBeatStream();
		hbss.init(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY)),
				Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT)),
				NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST)),
				Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_INITIALDELAY)),
				Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PINGDELAY)), "");
		// Start Resources gathering via heart beat resources
		// status update.
		hbss.start();

		// Execute when request arrives.
		esstream.execute(() -> {
			try (var ss = new ServerSocket(
					Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT)));) {
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
						// Execute concurrently through thread pool
						// executors.
						es.execute(new StreamPipelineTaskScheduler(cf, new String(bytesl.get(1)), bytesl.get(0),
								arguments, s));
					} catch (Exception ex) {
						log.error("Launching Stream Task scheduler error, See cause below \n", ex);
					}
				}
			} catch (Exception ex) {

			}
		});
		Utils.addShutdownHook(() -> {
			try {
				log.debug("Destroying...");
				if (!Objects.isNull(hbss)) {
					try {
						hbss.close();
					} catch (IOException e) {
						log.error(MDCConstants.EMPTY, e);
					}
				}
				if (!Objects.isNull(es)) {
					es.shutdown();
				}
				if (!Objects.isNull(esstream)) {
					esstream.shutdown();
				}
				if (!Objects.isNull(su)) {
					su.stop();
					su.destroy();
				}
				cdl.countDown();
				log.info("Program terminated...");
			} catch (Exception e) {
				log.error(MDCConstants.EMPTY, e);
			}
		});
		String streamport = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT);
		String streamwebport = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_WEB_PORT);
		log.info("Program kickoff amidst port Stream[port={},webport={}]", streamport, streamwebport);
		cdl.await();
	}

	public static void executeIncompleteJobs(Job job) throws Exception {
		StreamJobScheduler js = new StreamJobScheduler();
		js.schedule(job);
	}

	public static void closeResources() {
		if (!Objects.isNull(hbss)) {
			try {
				hbss.close();
				hbss = null;
			} catch (IOException e) {
				log.error(MDCConstants.EMPTY, e);
			}
		}
		if (!Objects.isNull(es)) {
			es.shutdown();
			es = null;
		}
		if (!Objects.isNull(esstream)) {
			esstream.shutdown();
		}
		if (!Objects.isNull(ss)) {
			try {
				ss.close();
				ss = null;
			} catch (IOException e) {
				log.error(MDCConstants.EMPTY, e);
			}
		}

		if (!Objects.isNull(channel)) {
			channel.close();
			channel = null;
		}
	}
}
