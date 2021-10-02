package com.github.mdc.tasks.executor;

import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.github.mdc.common.ByteArrayOutputStreamPool;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.CacheUtils;
import com.github.mdc.common.HeartBeatServerStream;
import com.github.mdc.common.HeartBeatTaskScheduler;
import com.github.mdc.common.HeartBeatTaskSchedulerStream;
import com.github.mdc.common.JobApp;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.LoadJar;
import com.github.mdc.common.MDCCache;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCMapReducePhaseClassLoader;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.ServerUtils;
import com.github.mdc.common.Utils;
import com.github.mdc.common.WebResourcesServlet;
import com.github.mdc.common.ZookeeperOperations;
import com.github.mdc.tasks.executor.web.NodeWebServlet;
import com.github.mdc.tasks.executor.web.ResourcesMetricsServlet;

public class MassiveDataTaskExecutorDeamon implements MassiveDataTaskExecutorDeamonMBean {

	static Logger log = Logger.getLogger(MassiveDataTaskExecutorDeamon.class);
	Map<String,Object> apptaskexecutormap = new ConcurrentHashMap<>();
	Map<String,Object> jobstageexecutormap = new ConcurrentHashMap<>();
	ConcurrentMap<String, OutputStream> resultstream = new ConcurrentHashMap<>();
	Map<String, HeartBeatTaskScheduler> hbtsappid = new ConcurrentHashMap<>();
	Map<String, HeartBeatTaskSchedulerStream> hbtssjobid = new ConcurrentHashMap<>();
	Map<String, HeartBeatServerStream> containeridhbss = new ConcurrentHashMap<>();
	Map<String,Map<String,Object>> jobidstageidexecutormap = new ConcurrentHashMap<>();
	Map<String,JobStage> jobidstageidjobstagemap = new ConcurrentHashMap<>();
	Queue<Object> taskqueue = new LinkedBlockingQueue<Object>();
	CuratorFramework cf;
	ServerSocket server;

	public static void main(String[] args) throws Exception {
		if (args == null || args.length != 1) {
			log.debug("Args" + args);
			if (args != null) {
				log.debug("Args Not of Length 1!=" + args.length);
				for (var arg : args) {
					log.debug(arg);
				}
			}
			System.exit(1);
		}
		if (args.length == 1) {
			log.debug("Args = ");
			for (var arg : args) {
				log.debug(arg);
			}
		}
		if (args[0].equals(MDCConstants.TEPROPLOADDISTROCONFIG)) {
			Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.BACKWARD_SLASH
					+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.BACKWARD_SLASH, MDCConstants.MDC_PROPERTIES);
		} else if (args[0].equals(MDCConstants.TEPROPLOADCLASSPATHCONFIG)) {
			Utils.loadLog4JSystemPropertiesClassPath(MDCConstants.MDC_TEST_PROPERTIES);
		} else if (args[0].equals(MDCConstants.TEPROPLOADCLASSPATHCONFIGEXCEPTION)) {
			Utils.loadLog4JSystemPropertiesClassPath(MDCConstants.MDC_TEST_EXCEPTION_PROPERTIES);
		}
		ByteBufferPoolDirect.init(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.BYTEBUFFERPOOL_MAX, MDCConstants.BYTEBUFFERPOOL_MAX_DEFAULT)));
		ByteArrayOutputStreamPool.init(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.BYTEBUFFERPOOL_MAX, MDCConstants.BYTEBUFFERPOOL_MAX_DEFAULT)));
		CacheUtils.initCache();
		var mdted = new MassiveDataTaskExecutorDeamon();
		mdted.init();
		mdted.start();
		log.info("MassiveDataTaskExecutorDeamon started at port....."
				+ System.getProperty(MDCConstants.TASKEXECUTOR_PORT));
		log.info("Adding Shutdown Hook...");
		Utils.addShutdownHook(() -> {
			try {
				log.info("Stopping and closes all the connections...");
				mdted.destroy();
				log.info("Freed the resources...");
				Runtime.getRuntime().halt(0);
			} catch (Exception e) {
				log.debug("", e);
			}
		});
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
								MDCConstants.ZK_BASE_PATH + MDCConstants.BACKWARD_SLASH + MDCConstants.TASKEXECUTOR,
								null, null);
						if (!nodesdata.contains(nodedata)) {
							ZookeeperOperations.ephemeralSequentialCreate.invoke(cf,
									MDCConstants.ZK_BASE_PATH + MDCConstants.BACKWARD_SLASH + MDCConstants.TASKEXECUTOR,
									MDCConstants.TE + MDCConstants.HYPHEN, nodedata);
						}
					}
				});

		ZookeeperOperations.ephemeralSequentialCreate.invoke(cf,
				MDCConstants.ZK_BASE_PATH + MDCConstants.BACKWARD_SLASH + MDCConstants.TASKEXECUTOR,
				MDCConstants.TE + MDCConstants.HYPHEN,
				NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST))
						+ MDCConstants.UNDERSCORE + MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PORT));
		
	}	
	ClassLoader cl;
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public void start() throws Exception {
		var threadpool = Executors.newCachedThreadPool();
		var launchtaskpool = Executors.newCachedThreadPool();
		var taskpool = Executors.newCachedThreadPool();
		var port = Integer.parseInt(System.getProperty(MDCConstants.TASKEXECUTOR_PORT));
		log.info("TaskExecutor Port: "+port);
		var su = new ServerUtils();
		su.init(port+50,
				new NodeWebServlet(new ConcurrentHashMap<String, Map<String,Process>>()), MDCConstants.BACKWARD_SLASH + MDCConstants.ASTERIX,
				new WebResourcesServlet(), MDCConstants.BACKWARD_SLASH +MDCConstants.RESOURCES+MDCConstants.BACKWARD_SLASH+ MDCConstants.ASTERIX,
				new ResourcesMetricsServlet(), MDCConstants.BACKWARD_SLASH +MDCConstants.DATA+MDCConstants.BACKWARD_SLASH+ MDCConstants.ASTERIX
				);
		su.start();
		server = new ServerSocket(port,256,InetAddress.getByAddress(new byte[] { 0x00, 0x00, 0x00, 0x00 }));
		var configuration = new Configuration();
		
		var inmemorycache = MDCCache.get();
		Semaphore semaphore = new Semaphore(1);
		cl = Thread.currentThread().getContextClassLoader();
		threadpool.execute(() -> {
			while (true) {
				try {
					var socket = server.accept();
					var deserobj = Utils.readObject(socket, cl);
					if (deserobj instanceof LoadJar loadjar) {
						log.info("Loading the Required jars");
						synchronized (deserobj) {
							var clsloader = MDCMapReducePhaseClassLoader
									.newInstance(loadjar.mrjar, cl);
							cl = clsloader;
						}
						log.info("Loaded the Required jars");
						socket.close();
					} else if (deserobj instanceof JobApp jobapp){
						semaphore.acquire();
						if(jobapp.getJobtype() == JobApp.JOBAPP.MR) {							
							if(!Objects.isNull(jobapp.getJobappid())&&Objects.isNull(hbtsappid.get(jobapp.getJobappid()))) {
								var hbts = new HeartBeatTaskScheduler();
								hbts.init(0,
										port,
										NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)),
										0,
										Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PINGDELAY)),"",
										jobapp.getJobappid(),"");
								hbtsappid.put(jobapp.getJobappid(), hbts);
							}							
						}	else if(jobapp.getJobtype() == JobApp.JOBAPP.STREAM){							
							if(!Objects.isNull(jobapp.getJobappid())&&Objects.isNull(hbtssjobid.get(jobapp.getJobappid()))) {
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
						semaphore.release();
					} else if(!Objects.isNull(deserobj)) {
						launchtaskpool.execute(new TaskExecutor(socket, cl, port, taskpool, configuration,
								apptaskexecutormap, jobstageexecutormap, resultstream, inmemorycache, deserobj,
								hbtsappid, hbtssjobid, containeridhbss,
								jobidstageidexecutormap,
								taskqueue, jobidstageidjobstagemap));
					}
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
						taskresultqueue.offer(eft);
					}
					Thread.sleep(300);
				} catch (Exception e) {
				} finally {
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
				} catch (Exception e) {
				} 
			}

		});
	}

	@Override
	public void destroy() throws Exception {
		hbtsappid.keySet().stream()
		.filter(key->!Objects.isNull(hbtsappid.get(key)))
		.forEach(key->{
			try {
				hbtsappid.remove(key).close();
			} catch (Exception e2) {
			}
		});
		hbtssjobid.keySet().stream()
		.filter(key->!Objects.isNull(hbtssjobid.get(key)))
		.forEach(key->{
			try {
				hbtssjobid.remove(key).close();
			} catch (Exception e1) {			
			}
		});
		containeridhbss.keySet().stream()
		.filter(key->!Objects.isNull(containeridhbss.get(key)))
		.forEach(key->{
			try {
				containeridhbss.remove(key).close();
			} catch (Exception e) {				
			}
		});
		if (cf != null) {
			cf.close();
		}
	}
	static class ExecutorsFutureTask{
		@SuppressWarnings("rawtypes")
		Future future;
		ExecutorService estask;
	}
	
}
