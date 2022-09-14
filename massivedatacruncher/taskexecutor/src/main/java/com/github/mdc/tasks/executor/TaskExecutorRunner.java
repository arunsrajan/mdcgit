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
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.*;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryonetty.ServerEndpoint;
import com.esotericsoftware.kryonetty.network.ConnectEvent;
import com.esotericsoftware.kryonetty.network.DisconnectEvent;
import com.esotericsoftware.kryonetty.network.ReceiveEvent;
import com.esotericsoftware.kryonetty.network.handler.NetworkHandler;
import com.esotericsoftware.kryonetty.network.handler.NetworkListener;
import com.github.mdc.common.*;
import io.netty.channel.ChannelHandlerContext;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.slf4j.LoggerFactory;

import com.github.mdc.tasks.executor.web.NodeWebServlet;
import com.github.mdc.tasks.executor.web.ResourcesMetricsServlet;

import static java.util.Objects.nonNull;

public class TaskExecutorRunner implements TaskExecutorRunnerMBean {

	static org.slf4j.Logger log = LoggerFactory.getLogger(TaskExecutorRunner.class);
	Map<String, Object> apptaskexecutormap = new ConcurrentHashMap<>();
	Map<String, Object> jobstageexecutormap = new ConcurrentHashMap<>();
	ConcurrentMap<String, OutputStream> resultstream = new ConcurrentHashMap<>();
	Map<String, HeartBeatTaskScheduler> hbtsappid = new ConcurrentHashMap<>();
	Map<String, HeartBeatTaskSchedulerStream> hbtssjobid = new ConcurrentHashMap<>();
	Map<String, HeartBeatStream> containeridhbss = new ConcurrentHashMap<>();
	Map<String, Map<String, Object>> jobidstageidexecutormap = new ConcurrentHashMap<>();
	Map<String, JobStage> jobidstageidjobstagemap = new ConcurrentHashMap<>();
	Queue<Object> taskqueue = new LinkedBlockingQueue<Object>();
	CuratorFramework cf;
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
		CacheUtils.initCache();
		int numberofprocessors = Runtime.getRuntime().availableProcessors();
		es = Executors.newCachedThreadPool();
		var mdted = new TaskExecutorRunner();
		mdted.init();
		mdted.start();
		log.info("TaskExecuterRunner started at port....."
				+ System.getProperty(MDCConstants.TASKEXECUTOR_PORT));
		log.info("Adding Shutdown Hook...");
		shutdown.await();
		try {
			if(nonNull(server)){
				server.close();
			}
			log.info("Stopping and closes all the connections...");
			mdted.destroy();
			ByteBufferPoolDirect.destroy();
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
	static ServerEndpoint server = null;
	@SuppressWarnings({"rawtypes"})
	@Override
	public void start() throws Exception {
		var launchtaskpool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		var port = Integer.parseInt(System.getProperty(MDCConstants.TASKEXECUTOR_PORT));
		log.info("TaskExecutor Port: " + port);
		var su = new ServerUtils();
		su.init(port + 50,
				new NodeWebServlet(new ConcurrentHashMap<String, Map<String, Process>>()), MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX,
				new WebResourcesServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.RESOURCES + MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX,
				new ResourcesMetricsServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.DATA + MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX
		);
		su.start();
		var configuration = new Configuration();

		var inmemorycache = MDCCache.get();
		cl = TaskExecutorRunner.class.getClassLoader();
		log.info("Default Class Loader: "+cl);

		server = Utils.getServerKryoNetty(port, new NetworkListener() {

			@NetworkHandler
			public void onConnect(ConnectEvent event) {
				ChannelHandlerContext ctx = event.getCtx();
				log.info("Client: Connected to server: " + ctx.channel().remoteAddress());
			}

			@NetworkHandler
			public void onDisconnect(DisconnectEvent event) {
				ChannelHandlerContext ctx = event.getCtx();
				log.info("Server: Client disconnected: " + ctx.channel().remoteAddress());
			}

			@NetworkHandler
			public void onReceive(ReceiveEvent event) {
				try{
					Object deserobj =event.getObject();
					log.info("Deserialized Object: "+deserobj);
					if(deserobj instanceof TaskExecutorShutdown){
						shutdown.countDown();
					}
					else if (deserobj instanceof LoadJar loadjar) {
						log.info("Loading the Required jars: "+loadjar.mrjar);
						cl = MDCMapReducePhaseClassLoader
									.newInstance(loadjar.mrjar, cl);
						log.info("Loaded the Required jars with Next registration ID: "+server.getKryoSerialization().obtainKryo().getNextRegistrationId());
						Kryo kryo = server.getKryoSerialization().obtainKryo();
						kryo.setClassLoader(cl);
						server.getKryoSerialization().free(kryo);
						if(nonNull(loadjar.classes) && !loadjar.classes.isEmpty()) {
							int classindex = 1;
							for(String clz:loadjar.classes) {
								var main = Class.forName(clz, true, cl);
								kryo = server.getKryoSerialization().obtainKryo();
								kryo.register(main, new CompatibleFieldSerializer(kryo, main), 1000 + classindex);
								classindex++;
								log.info("Next registration ID: "+kryo.getNextRegistrationId()+" for class "+clz);
								server.getKryoSerialization().free(kryo);
							}
						}
						server.send(event.getCtx(),  MDCConstants.JARLOADED);
					} else if (deserobj instanceof JobApp jobapp) {
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
							HeartBeatStream hbss = new HeartBeatStream();
							var teport = Integer
									.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PORT));
							var pingdelay = Integer
									.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PINGDELAY));
							var host = NetworkUtil
									.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST));
							log.info("Starting Hearbeat for container id: {} with host {} and port {}",containerid,host,teport);
							hbss.init(0, teport, host, 0, pingdelay, containerid);
							hbss.ping();
							containeridhbss.put(containerid, hbss);
						}
					} else if (!Objects.isNull(deserobj)) {
						launchtaskpool.execute(new TaskExecutor(server, cl, port, es, configuration,
								apptaskexecutormap, jobstageexecutormap, resultstream, inmemorycache, deserobj,
								hbtsappid, hbtssjobid, containeridhbss,
								jobidstageidexecutormap,
								taskqueue, jobidstageidjobstagemap, event));
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
