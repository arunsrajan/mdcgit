package com.github.mdc.tasks.scheduler.executor.standalone;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.github.mdc.common.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ServerCnxnFactory;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryonetty.ServerEndpoint;
import com.esotericsoftware.kryonetty.network.ReceiveEvent;
import com.esotericsoftware.kryonetty.network.handler.NetworkHandler;
import com.esotericsoftware.kryonetty.network.handler.NetworkListener;
import com.github.mdc.stream.scheduler.StreamPipelineTaskScheduler;
import com.github.mdc.tasks.executor.NodeRunner;
import com.github.mdc.tasks.executor.web.NodeWebServlet;
import com.github.mdc.tasks.executor.web.ResourcesMetricsServlet;
import com.github.mdc.tasks.scheduler.TaskScheduler;

public class EmbeddedSchedulersNodeLauncher {
	static Logger log = Logger.getLogger(EmbeddedSchedulersNodeLauncher.class);

	public static final String STOPPINGANDCLOSECONNECTION = "Stopping and closes all the connections...";

	public static void main(String[] args) throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		log.info(MDCScalaConstants.SCALA_VERSION());
		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, MDCConstants.MDC_PROPERTIES);
		var cdl = new CountDownLatch(3);
		var clientport = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_STANDALONE_CLIENTPORT,
				MDCConstants.ZOOKEEPER_STANDALONE_CLIENTPORT_DEFAULT));
		var numconnections = Integer
				.parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_STANDALONE_NUMCONNECTIONS,
						MDCConstants.ZOOKEEPER_STANDALONE_NUMCONNECTIONS_DEFAULT));
		var ticktime = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_STANDALONE_TICKTIME,
				MDCConstants.ZOOKEEPER_STANDALONE_TICKTIME_DEFAULT));
		ServerCnxnFactory scf = null;
		try (var cf = CuratorFrameworkFactory.newClient(
				MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_HOSTPORT), 20000, 50000, new RetryForever(
						Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_RETRYDELAY))));) {
			scf = Utils.startZookeeperServer(clientport, numconnections, ticktime);
			cf.start();
			cf.blockUntilConnected();
			ByteBufferPoolDirect.init();
			ByteBufferPool.init(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.BYTEBUFFERPOOL_MAX,
					MDCConstants.BYTEBUFFERPOOL_MAX_DEFAULT)));
			startTaskScheduler(cf, cdl);
			startTaskSchedulerStream(cf, cdl);
			startContainerLauncher(cdl);
			cdl.await();
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.error(MDCConstants.EMPTY, ex);
		}
		if (!Objects.isNull(scf)) {
			scf.closeAll();
		}
		Runtime.getRuntime().halt(0);
	}
	static ServerEndpoint server = null;
	@SuppressWarnings("resource")
	public static void startContainerLauncher(CountDownLatch cdl) {
		HeartBeatServerStream hbss = new HeartBeatServerStream();
		try {
			var port = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.NODE_PORT));
			var pingdelay = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PINGDELAY));
			var host = NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST));
			hbss.init(0, port, host, 0, pingdelay, "");
			var hb = new HeartBeatServer();
			hb.init(0, port, host, 0, pingdelay, "");
			hbss.ping();
			var escontainer = Executors.newWorkStealingPool();

			var hdfs = FileSystem.get(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
					new Configuration());
			var containerprocesses = new ConcurrentHashMap<String, Map<String, Process>>();
			var containeridthreads = new ConcurrentHashMap<String, Map<String, List<Thread>>>();
			var containeridports = new ConcurrentHashMap<String, List<Integer>>();
			var su = new ServerUtils();
			su.init(port + 50, new NodeWebServlet(containerprocesses),
					MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX, new WebResourcesServlet(),
					MDCConstants.FORWARD_SLASH + MDCConstants.RESOURCES + MDCConstants.FORWARD_SLASH
							+ MDCConstants.ASTERIX,
					new ResourcesMetricsServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.DATA
							+ MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX);
			su.start();
			server = Utils.getServerKryoNetty(port,
					new NetworkListener() {
					@NetworkHandler
		            public void onReceive(ReceiveEvent event) {
						try {
							Object object = event.getObject();
							var container = new NodeRunner(server, MDCConstants.PROPLOADERCONFIGFOLDER,
									containerprocesses, hdfs, containeridthreads, containeridports,
									object, event);
							Future<Boolean> containerallocated = escontainer.submit(container);
							log.info("Containers Allocated: " + containerallocated.get());
						} catch (InterruptedException e) {
							log.warn("Interrupted!", e);
							// Restore interrupted state...
							Thread.currentThread().interrupt();
						} catch (Exception e) {
							log.error(MDCConstants.EMPTY, e);
						}
					}
				});
			log.debug("NodeLauncher started at port....." + MDCProperties.get().getProperty(MDCConstants.NODE_PORT));
			log.debug("Adding Shutdown Hook...");
			Utils.addShutdownHook(() -> {
				try {
					containerprocesses
							.keySet().stream().map(containerprocesses::get).flatMap(mapproc -> mapproc.keySet().stream()
									.map(key -> mapproc.get(key)).collect(Collectors.toList()).stream())
							.forEach(proc -> {
								log.debug("Destroying the Container Process: " + proc);
								proc.destroy();
							});
					log.debug(STOPPINGANDCLOSECONNECTION);
					log.debug("Destroying...");
					if (!Objects.isNull(hbss)) {
						hbss.close();
					}
					if (!Objects.isNull(hdfs)) {
						hdfs.close();
					}
					if (!Objects.isNull(server)) {
						server.close();
					}
					cdl.countDown();
				} catch (Exception e) {
					log.debug("", e);
				}
			});
		} catch (Exception ex) {
			log.error("Unable to start Node Manager due to ", ex);
		}
	}

	public static void startTaskSchedulerStream(CuratorFramework cf, CountDownLatch cdl) throws Exception {
		var esstream = Executors.newFixedThreadPool(1);
		var es = Executors.newWorkStealingPool();
		var su = new ServerUtils();
		su.init(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_WEB_PORT)),
				new TaskSchedulerWebServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX,
				new WebResourcesServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.RESOURCES
						+ MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX);
		su.start();
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
		var hbss = new HeartBeatServerStream();
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
			try (var ss = Utils.createSSLServerSocket(
					Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT)));) {
				while (true) {
					try {
						var s = ss.accept();
						var bytesl = new ArrayList<byte[]>();
						var kryo = Utils.getKryoSerializerDeserializer();
						var input = new Input(s.getInputStream());
						log.debug("Obtaining Input Objects From Submitter");
						while (true) {
							var obj = kryo.readClassAndObject(input);
							log.debug("Input Object: " + obj);
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
						log.info("Launching Stream Task scheduler error, See cause below \n", ex);
					}
				}
			} catch (Exception ex) {

			}
		});
		Utils.addShutdownHook(() -> {
			try {
				log.debug(STOPPINGANDCLOSECONNECTION);
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
				log.info("Halting...");
			} catch (Exception e) {
				log.error(MDCConstants.EMPTY, e);
			}
		});
	}

	@SuppressWarnings({ "unchecked", "resource" })
	public static void startTaskScheduler(CuratorFramework cf, CountDownLatch cdl) throws Exception {
		var hbs = new HeartBeatServer();
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

		boolean ishdfs = Boolean.parseBoolean(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_ISHDFS));
		var ss = Utils.createSSLServerSocket(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT)));
		essingle.execute(() -> {
			while (true) {
				try {
					var s = ss.accept();
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
						if (!baoss.isEmpty()) {
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
		Utils.addShutdownHook(() -> {
			try {
				log.debug(STOPPINGANDCLOSECONNECTION);
				hbs.stop();
				hbs.destroy();
				log.debug("Destroying...");
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
	}
}
