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
package com.github.mdc.tasks.executor;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.*;

import com.github.mdc.common.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.log4j.Logger;

import com.github.mdc.tasks.executor.web.NodeWebServlet;
import com.github.mdc.tasks.executor.web.ResourcesMetricsServlet;

public class TaskExecutorRunner implements TaskExecutorRunnerMBean {

	static Logger log = Logger.getLogger(TaskExecutorRunner.class);
	Map<String, Object> apptaskexecutormap = new ConcurrentHashMap<>();
	Map<String, Object> jobstageexecutormap = new ConcurrentHashMap<>();
	ConcurrentMap<String, OutputStream> resultstream = new ConcurrentHashMap<>();
	Map<String, HeartBeatTaskScheduler> hbtsappid = new ConcurrentHashMap<>();
	Map<String, HeartBeatTaskSchedulerStream> hbtssjobid = new ConcurrentHashMap<>();
	Map<String, HeartBeatServerStream> containeridhbss = new ConcurrentHashMap<>();
	Map<String, Map<String, Object>> jobidstageidexecutormap = new ConcurrentHashMap<>();
	Map<String, JobStage> jobidstageidjobstagemap = new ConcurrentHashMap<>();
	Queue<Object> taskqueue = new LinkedBlockingQueue<Object>();
	CuratorFramework cf;
	ServerSocket server;
	static ExecutorService es;
	static CountDownLatch shutdown = new CountDownLatch(1);

	public static void main(String[] args) throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		if (args == null || args.length != 2) {
			log.debug("Args" + args);
			if (args != null) {
				log.debug("Args Not of Length 2!=" + args.length);
				for (var arg : args) {
					log.debug(arg);
				}
			}
			System.exit(1);
		}
		if (args.length == 2) {
			log.debug("Args = ");
			for (var arg : args) {
				log.debug(arg);
			}
		}
		if (args[0].equals(MDCConstants.TEPROPLOADDISTROCONFIG)) {
			Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
					+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, MDCConstants.MDC_PROPERTIES);
		}
		ByteBufferPoolDirect.init();
		log.info("Direct Memory Allocated: " + args[1]);
		int directmemory = Integer.valueOf(args[1]) / 128;
		log.info("Number Of 128 MB directmemory: " + directmemory);
		ByteBufferPool.init(directmemory);
		CacheUtils.initCache();
		es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		var mdted = new TaskExecutorRunner();
		mdted.init();
		mdted.start();
		log.info("TaskExecuterRunner started at port....."
				+ System.getProperty(MDCConstants.TASKEXECUTOR_PORT));
		log.info("Adding Shutdown Hook...");
		shutdown.await();
		try {
			log.info("Stopping and closes all the connections...");
			mdted.destroy();
			ByteBufferPoolDirect.destroy();
			ByteBufferPool.destroyByteBuffer();
			log.info("Freed the resources...");
			Runtime.getRuntime().halt(0);
		} catch (Exception e) {
			log.error("", e);
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public void init() throws Exception {
		cf = CuratorFrameworkFactory.newClient(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_HOSTPORT), 20000,
				50000,
				new RetryForever(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_RETRYDELAY))));
		cf.start();
		ZookeeperOperations.addconnectionstate.addConnectionStateListener(cf,
				(CuratorFramework cf, ConnectionState cs) -> {
					if (cs == ConnectionState.RECONNECTED) {
						var nodedata = NetworkUtil
								.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST))
								+ MDCConstants.UNDERSCORE
								+ MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PORT);
						var nodesdata = (List<String>) ZookeeperOperations.nodesdata.invoke(cf,
								MDCConstants.ZK_BASE_PATH + MDCConstants.FORWARD_SLASH + MDCConstants.TASKEXECUTOR,
								null, null);
						if (!nodesdata.contains(nodedata)) {
							ZookeeperOperations.ephemeralSequentialCreate.invoke(cf,
									MDCConstants.ZK_BASE_PATH + MDCConstants.FORWARD_SLASH + MDCConstants.TASKEXECUTOR,
									MDCConstants.TE + MDCConstants.HYPHEN, nodedata);
						}
					}
				});

		ZookeeperOperations.ephemeralSequentialCreate.invoke(cf,
				MDCConstants.ZK_BASE_PATH + MDCConstants.FORWARD_SLASH + MDCConstants.TASKEXECUTOR,
				MDCConstants.TE + MDCConstants.HYPHEN,
				NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST))
						+ MDCConstants.UNDERSCORE + MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PORT));

	}
	ClassLoader cl;

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Override
	public void start() throws Exception {
		var threadpool = Executors.newSingleThreadExecutor();
		var launchtaskpool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		var taskpool = Executors.newFixedThreadPool(2);
		var port = Integer.parseInt(System.getProperty(MDCConstants.TASKEXECUTOR_PORT));
		log.info("TaskExecutor Port: " + port);
		var su = new ServerUtils();
		su.init(port + 50,
				new NodeWebServlet(new ConcurrentHashMap<String, Map<String, Process>>()), MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX,
				new WebResourcesServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.RESOURCES + MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX,
				new ResourcesMetricsServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.DATA + MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX
		);
		su.start();
		server = Utils.createSSLServerSocket(port);
		var configuration = new Configuration();

		var inmemorycache = MDCCache.get();
		Semaphore semaphore = new Semaphore(1);		
		cl = TaskExecutorRunner.class.getClassLoader();
		log.info("Default Class Loader: "+cl);
		threadpool.execute(() -> {
			while (true) {
				try {					
					var socket = server.accept();					
					log.info("Default Class Loader: "+cl);
					var deserobj = Utils.readObject(socket, cl);
					log.info("Deserialized Object: "+deserobj);
					if(deserobj instanceof TaskExecutorShutdown){
						shutdown.countDown();
						socket.close();
						break;
					}
					else if (deserobj instanceof LoadJar loadjar) {
						log.info("Loading the Required jars: "+loadjar.mrjar);
						semaphore.acquire();
						cl = MDCMapReducePhaseClassLoader
									.newInstance(loadjar.mrjar, cl);
						log.info("Loaded the Required jars");
						Utils.writeObject(socket, LoadJar.class.getSimpleName());
						semaphore.release();	
						socket.close();
					} else if (deserobj instanceof JobApp jobapp) {
						semaphore.acquire();
						if (jobapp.getJobtype() == JobApp.JOBAPP.MR) {
							if (!Objects.isNull(jobapp.getJobappid()) && Objects.isNull(hbtsappid.get(jobapp.getJobappid()))) {
								var hbts = new HeartBeatTaskScheduler();
								hbts.init(0,
										port,
										NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)),
										0,
										Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PINGDELAY)), "",
										jobapp.getJobappid(), "");
								hbtsappid.put(jobapp.getJobappid(), hbts);
							}
						}	else if (jobapp.getJobtype() == JobApp.JOBAPP.STREAM) {
							if (!Objects.isNull(jobapp.getJobappid()) && Objects.isNull(hbtssjobid.get(jobapp.getJobappid()))) {
								var hbtss = new HeartBeatTaskSchedulerStream();
								hbtss.init(0, Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PORT)),
										NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)), 0,
										Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PINGDELAY)),
										MDCConstants.EMPTY,
										jobapp.getJobappid());
								hbtssjobid.put(jobapp.getJobappid(), hbtss);
							}
						}
						var containerid = (String) jobapp.getContainerid();
						if (Objects.isNull(containeridhbss.get(containerid))) {
							HeartBeatServerStream hbss = new HeartBeatServerStream();
							var teport = Integer
									.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PORT));
							var pingdelay = Integer
									.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PINGDELAY));
							var host = NetworkUtil
									.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST));
							log.info("Starting Hearbeat for container id: " + containerid);
							hbss.init(0, teport, host, 0, pingdelay, containerid);
							hbss.ping();
							containeridhbss.put(containerid, hbss);
						}
						Utils.writeObject(socket, JobApp.class.getSimpleName());
						socket.close();
						semaphore.release();
					} else if (!Objects.isNull(deserobj)) {
						launchtaskpool.execute(new TaskExecutor(socket, cl, port, es, configuration,
								apptaskexecutormap, jobstageexecutormap, resultstream, inmemorycache, deserobj,
								hbtsappid, hbtssjobid, containeridhbss,
								jobidstageidexecutormap,
								taskqueue, jobidstageidjobstagemap));
					} else {
						socket.close();
					}
				} catch (InterruptedException e) {
					log.warn("Interrupted!", e);
					// Restore interrupted state...
					Thread.currentThread().interrupt();
				} catch (Exception ex) {
					log.info(MDCConstants.EMPTY, ex);
				}
			}
		});
		var taskresultqueue = new LinkedBlockingQueue<ExecutorsFutureTask>();
		taskpool.execute(() -> {
			while (true) {
				try {
					if (!taskqueue.isEmpty()) {
						ExecutorService estask = Executors.newScheduledThreadPool(1);
						Future future = estask.submit((Callable) taskqueue.poll());
						ExecutorsFutureTask eft = new ExecutorsFutureTask();
						eft.future = future;
						eft.estask = estask;
						if (!taskresultqueue.offer(eft)) {
							log.info("Task Exceution Queue Full");
						}
					}
					Thread.sleep(300);
				} catch (InterruptedException e) {
					log.warn("Interrupted!", e);
					// Restore interrupted state...
					Thread.currentThread().interrupt();
				} catch (Exception e) {
				}
			}
		});
		taskpool.execute(() -> {
			while (true) {
				try {
					if (!taskresultqueue.isEmpty()) {
						ExecutorsFutureTask eft = taskresultqueue.poll();
						eft.future.get();
						eft.estask.shutdown();
					}
					Thread.sleep(300);
				} catch (InterruptedException e) {
					log.warn("Interrupted!", e);
					// Restore interrupted state...
					Thread.currentThread().interrupt();
				} catch (Exception e) {
				}
			}

		});
	}

	@Override
	public void destroy() throws Exception {
		hbtsappid.keySet().stream()
				.filter(key -> !Objects.isNull(hbtsappid.get(key)))
				.forEach(key -> {
					try {
						hbtsappid.remove(key).close();
					} catch (Exception e2) {
					}
				});
		hbtssjobid.keySet().stream()
				.filter(key -> !Objects.isNull(hbtssjobid.get(key)))
				.forEach(key -> {
					try {
						hbtssjobid.remove(key).close();
					} catch (Exception e1) {
					}
				});
		containeridhbss.keySet().stream()
				.filter(key -> !Objects.isNull(containeridhbss.get(key)))
				.forEach(key -> {
					try {
						containeridhbss.remove(key).close();
					} catch (Exception e) {
					}
				});
		if (cf != null) {
			cf.close();
		}
		if (es != null) {
			es.shutdownNow();
			es.awaitTermination(1, TimeUnit.SECONDS);
		}
	}

	static class ExecutorsFutureTask {
		@SuppressWarnings("rawtypes")
		Future future;
		ExecutorService estask;
	}

}
