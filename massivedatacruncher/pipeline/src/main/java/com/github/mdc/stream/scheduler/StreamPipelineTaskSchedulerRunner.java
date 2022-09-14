package com.github.mdc.stream.scheduler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.log4j.Logger;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.xerial.snappy.SnappyInputStream;

import com.esotericsoftware.kryo.io.Input;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.CacheUtils;
import com.github.mdc.common.HeartBeatStream;
import com.github.mdc.common.Job;
import com.github.mdc.common.LoadJar;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCMapReducePhaseClassLoader;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.ServerUtils;
import com.github.mdc.common.TaskSchedulerWebServlet;
import com.github.mdc.common.TssHAChannel;
import com.github.mdc.common.TssHAHostPorts;
import com.github.mdc.common.Utils;
import com.github.mdc.common.WebResourcesServlet;
import com.github.mdc.common.ZookeeperOperations;

/**
 * 
 * @author Arun The task scheduler daemon process.
 */
public class StreamPipelineTaskSchedulerRunner {
	static Logger log = Logger.getLogger(StreamPipelineTaskSchedulerRunner.class);
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

		try (var cf = CuratorFrameworkFactory.newClient(
				MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_HOSTPORT), 20000, 50000,
				new RetryForever(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_RETRYDELAY))));
				final ServerSocket server = Utils.createSSLServerSocket(
						Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT)) + 20);) {

			cf.start();
			cf.blockUntilConnected();
			ByteBufferPoolDirect.init();
			try (LeaderLatch ll = new LeaderLatch(cf,
					MDCConstants.FORWARD_SLASH + MDCProperties.get().getProperty(MDCConstants.CLUSTERNAME)
							+ MDCConstants.FORWARD_SLASH + MDCConstants.TSS)) {
				LeaderLatchListener lllistener = new LeaderLatchListener() {
					@Override
					public void isLeader() {
						try {
							closeResources();
							Thread.sleep(4000);
							esstream = Executors.newWorkStealingPool();
							es = Executors.newWorkStealingPool();

							var su = new ServerUtils();
							su.init(Integer.parseInt(
									MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_WEB_PORT)),
									new TaskSchedulerWebServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX,
									new WebResourcesServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.RESOURCES
											+ MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX);
							su.start();
							if (!(boolean) ZookeeperOperations.checkexists.invoke(cf,
									MDCConstants.FORWARD_SLASH + MDCProperties.get().getProperty(
											MDCConstants.CLUSTERNAME) + MDCConstants.FORWARD_SLASH + MDCConstants.TSS,
									MDCConstants.LEADER,
									NetworkUtil.getNetworkAddress(
											MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST))
											+ MDCConstants.UNDERSCORE
											+ MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT))) {
								ZookeeperOperations.persistentCreate
										.invoke(cf,
												MDCConstants.FORWARD_SLASH
														+ MDCProperties.get().getProperty(MDCConstants.CLUSTERNAME)
														+ MDCConstants.FORWARD_SLASH + MDCConstants.TSS,
												MDCConstants.LEADER,
												NetworkUtil
														.getNetworkAddress(MDCProperties.get()
																.getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST))
														+ MDCConstants.UNDERSCORE + MDCProperties.get()
																.getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT));
							} else {
								ZookeeperOperations.writedata.invoke(cf,
										MDCConstants.FORWARD_SLASH + MDCProperties.get()
												.getProperty(MDCConstants.CLUSTERNAME) + MDCConstants.FORWARD_SLASH
												+ MDCConstants.TSS + MDCConstants.FORWARD_SLASH + MDCConstants.LEADER,
										MDCConstants.EMPTY,
										NetworkUtil.getNetworkAddress(
												MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST))
												+ MDCConstants.UNDERSCORE + MDCProperties.get()
														.getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT));
							}
							hbss = new HeartBeatStream();
							hbss.init(
									Integer.parseInt(MDCProperties.get()
											.getProperty(MDCConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY)),
									Integer.parseInt(
											MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT)),
									NetworkUtil.getNetworkAddress(
											MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST)),
									Integer.parseInt(MDCProperties.get()
											.getProperty(MDCConstants.TASKSCHEDULERSTREAM_INITIALDELAY)),
									Integer.parseInt(MDCProperties.get()
											.getProperty(MDCConstants.TASKSCHEDULERSTREAM_PINGDELAY)),
									"");
							// Start Resources gathering via heart beat resources
							// status update.
							hbss.start();
							ss = Utils.createSSLServerSocket(
									Integer.parseInt(
											MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT)));
							// Execute when request arrives.
							esstream.execute(() -> {
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
										es.execute(new StreamPipelineTaskScheduler(cf, new String(bytesl.get(1)),
												bytesl.get(0), arguments, s));
									} catch (Exception ex) {
										log.info("Launching Stream Task scheduler error, See cause below \n", ex);
									}
								}
							});
							String schhostport = NetworkUtil.getNetworkAddress(
									MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST))
									+ MDCConstants.UNDERSCORE
									+ (Integer.parseInt(
											MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT))
											+ 20);
							JChannel chtssha = Utils.getChannelTSSHA(schhostport, new Receiver() {

								@Override
								public void viewAccepted(View clusterview) {
									log.info("Nodes View: " + clusterview.getMembers());
									var tsshahostports = clusterview.getMembers().stream()
											.map(address -> address.toString())
											.filter(addresss -> !addresss.equals(schhostport))
											.collect(Collectors.toList());
									TssHAHostPorts.set(tsshahostports);
								}

								@Override
								public void receive(Message msg) {

								}
							});
							TssHAChannel.tsshachannel = chtssha;
							log.info("Stream Scheduler started at..... 0.0.0.0:"
									+ MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT));
							Set<String> jobkeys = jobidjobmap.keySet();
							for (String jobkey : jobkeys) {
								Job job = jobidjobmap.remove(jobkey);
								if (!job.isIscompleted()) {
									log.info("Executing Job....." + job);
									executeIncompleteJobs(job);
								}
							}
						} catch (InterruptedException e) {
							log.warn("Interrupted!", e);
						    // Restore interrupted state...
						    Thread.currentThread().interrupt();
						} catch (Exception ex) {
							log.info(MDCConstants.EMPTY, ex);
						}
					}

					@Override
					public void notLeader() {
						closeResources();
					}

				};
				ll.addListener(lllistener);
				ll.start();
				Thread.sleep(10000);
				cl = Thread.currentThread().getContextClassLoader();

				threadpool.execute(() -> {
					while (true) {
						try (var socket = server.accept();) {
							var deserobj = Utils.readObject(socket, cl);
							if (deserobj instanceof LoadJar loadjar) {
								log.info("Loading the Required jars");
								synchronized (deserobj) {
									var clsloader = MDCMapReducePhaseClassLoader.newInstance(loadjar.mrjar, cl);
									cl = clsloader;
								}
								log.info("Loaded the Required jars");
								Utils.writeObject(socket, MDCConstants.JARLOADED);
							}
						} catch (Exception ex) {
							log.info(MDCConstants.EMPTY, ex);
						}
					}
				});
				if (!ll.hasLeadership()) {
					log.info("Entered into standby state...........");
					channel = Utils.getChannelTSSHA(NetworkUtil
							.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST))
							+ MDCConstants.UNDERSCORE
							+ (Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT))
									+ 20),
							new Receiver() {

								@Override
								public void viewAccepted(View clusterview) {
									log.info("TSS HA View: " + clusterview.getMembers());
								}

								public void receive(Message msg) {
									try {
										log.info("Entered MassiveDataStreamTaskSchedulerDaemon.Receiver.receive");
										var rawbuffer = (byte[]) ((ObjectMessage) msg).getObject();
										var kryo = Utils.getKryoSerializerDeserializer();
										kryo.register(StreamPipelineTaskSubmitter.class);
										kryo.setClassLoader(cl);
										try (var bais = new ByteArrayInputStream(rawbuffer);
												var decompressor = new SnappyInputStream(bais);
												var input = new Input(decompressor);) {
											var job = (Job) Utils.readKryoInputObjectWithClass(kryo, input);
											jobidjobmap.put( job.getId(), job);
											log.info("Received Job: " + jobidjobmap);
											log.info("Exiting MassiveDataStreamTaskSchedulerDaemon.Receiver.receive");
										}
									} catch (Exception ex) {
										log.info(MDCConstants.EMPTY, ex);
									}
								}
							});
				}
				log.info("Adding Shutdown Hook...");
				Utils.addShutdownHook(() -> {
					try {
						log.debug("Stopping and closes all the connections...");
						log.debug("Destroying...");
						closeResources();
						cf.close();
						if (!Objects.isNull(threadpool)) {
							threadpool.shutdown();
							threadpool = null;
						}
						cdl.countDown();
						log.info("Halting...");
						Runtime.getRuntime().halt(0);
					} catch (Exception e) {
						log.error(MDCConstants.EMPTY, e);
					}
				});
				cdl.await();
			}
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.error("Launching Stream Task scheduler error, See cause below \n", ex);
		}
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
