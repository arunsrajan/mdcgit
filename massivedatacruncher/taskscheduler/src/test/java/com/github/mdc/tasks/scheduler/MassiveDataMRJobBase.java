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
package com.github.mdc.tasks.scheduler;

import java.io.InputStream;
import java.net.URI;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.IOUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.CacheUtils;
import com.github.mdc.common.HeartBeat;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.StreamDataCruncher;
import com.github.mdc.common.Utils;
import com.github.mdc.tasks.executor.NodeRunner;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;

public class MassiveDataMRJobBase {

	static Logger log = Logger.getLogger(MassiveDataMRJobBase.class);

	static HdfsLocalCluster hdfsLocalCluster;
	static int namenodeport = 9000;
	static int namenodehttpport = 50070;
	static FileSystem hdfs;
	String[] carrierheader = {"Code", "Description"};
	String hdfsfilepath = "hdfs://localhost:9000";
	String airlines = "/airlines";
	static String airlinenoheader = "/airlinenoheader";
	static String airlinesamplenoheader = "/airlinesamplenoheader";
	String airlineverysmall = "/airlineverysmall";
	String airlineveryverysmall1988 = "/airlineververysmall1988";
	String airlineveryverysmall1989 = "/airlineveryverysmall1989";
	String airlinepartitionsmall = "/airlinepartitionsmall";
	String airlinesmall = "/airlinesmall";
	static String airlinesample = "/airlinesample";
	static String csvfileextn = ".csv";
	static String txtfileextn = ".txt";
	static String jsonfileextn = ".json";
	String airline2 = "/airline2";
	static String airline1987 = "/airline1987";
	String airlinemedium = "/airlinemedium";
	String airlineveryverysmall = "/airlineveryverysmall";
	String airlineveryveryverysmall = "/airlineveryveryverysmall";
	static String airlinepairjoin = "/airlinepairjoin";
	static String wordcount = "/wordcount";
	static String population = "/population";
	static String carriers = "/carriers";
	static String cars = "/cars";
	String groupbykey = "/groupbykey";
	static String bicyclecrash = "/bicyclecrash";
	static String airlinemultiplefilesfolder = "/airlinemultiplefilesfolder";
	static String githubevents = "/githubevents";
	private static int numberofnodes = 1;
	private static int port;
	static List<HeartBeat> hbssl = new ArrayList<>();
	static ExecutorService executorpool;
	static int zookeeperport = 2181;

	private static TestingServer testingserver;

	private static Registry server;

	@BeforeClass
	public static void setServerUp() throws Exception {
		try (InputStream istream = MassiveDataMRJobBase.class.getResourceAsStream("/log4j.properties");) {
			System.setProperty("HIBCFG", "../config/mdchibernate.cfg.xml");
			System.setProperty("HADOOP_HOME", "C:\\DEVELOPMENT\\hadoop\\hadooplocal\\hadoop-3.3.1");
			PropertyConfigurator.configure(istream);
			Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
					+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, "mdctest.properties");
			ByteBufferPoolDirect.init();
			CacheUtils.initCache();
			testingserver = new TestingServer(zookeeperport);
			testingserver.start();
			executorpool = Executors.newWorkStealingPool();
			int rescheduledelay = Integer.parseInt(MDCProperties.get().getProperty("taskscheduler.rescheduledelay"));
			int initialdelay = Integer.parseInt(MDCProperties.get().getProperty("taskscheduler.initialdelay"));
			int pingdelay = Integer.parseInt(MDCProperties.get().getProperty("taskscheduler.pingdelay"));
			String host = NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty("taskscheduler.host"));
			port = Integer.parseInt(MDCProperties.get().getProperty("taskscheduler.port"));
			HeartBeat hb = new HeartBeat();
			hb.init(rescheduledelay, port, host, initialdelay, pingdelay, "");
			hb.start();
			Configuration configuration = new Configuration();
			hdfs = FileSystem.get(new URI(MDCProperties.get().getProperty("hdfs.namenode.url")),
					configuration);
			log.info("HDFS FileSystem Object: " + hdfs);
			if (numberofnodes > 0) {
				port = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.NODE_PORT));
				int executorsindex = 0;
				ConcurrentMap<String, Map<String, Process>> containerprocesses = new ConcurrentHashMap<>();
				ConcurrentMap<String, Map<String, List<Thread>>> containeridthreads = new ConcurrentHashMap<>();
				ExecutorService es = Executors.newWorkStealingPool();
				var containeridports = new ConcurrentHashMap<String, List<Integer>>();
				while (executorsindex < numberofnodes) {
					hb = new HeartBeat();
					host = NetworkUtil
							.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST));
					hb.init(rescheduledelay, port, host, initialdelay, pingdelay, "");
					hb.ping();
					hbssl.add(hb);
					Thread.sleep(3000);
					server = Utils.getRPCRegistry(port,
							new StreamDataCruncher() {
						public Object postObject(Object object)throws RemoteException {
								try {
									var container = new NodeRunner(MDCConstants.PROPLOADERCONFIGFOLDER,
											containerprocesses, hdfs, containeridthreads, containeridports,
											object);
									Future<Object> containerallocated = es.submit(container);
									Object returnresultobject = containerallocated.get();
									log.info("Containers Allocated: " + returnresultobject);
									return returnresultobject;
								} catch (InterruptedException e) {
									log.warn("Interrupted!", e);
									// Restore interrupted state...
									Thread.currentThread().interrupt();
								} catch (Exception e) {
									log.error(MDCConstants.EMPTY, e);
								}
								return null;
							}
						});
					port++;
					executorsindex++;
				}
			}

			uploadfile(hdfs, airlinesample, airlinesample + csvfileextn);
			uploadfile(hdfs, carriers, carriers + csvfileextn);
		} catch (Exception ex) {
			log.info("MRJobTestBase Initialization Error", ex);
		}
	}


	public static void uploadfile(FileSystem hdfs, String dir, String filename) throws Exception {
		InputStream is = MassiveDataMRJobBase.class.getResourceAsStream(filename);
		String jobpath = dir;
		String filepath = jobpath + filename;
		Path jobpathurl = new Path(jobpath);
		if (!hdfs.exists(jobpathurl)) {
			hdfs.mkdirs(jobpathurl);
		}
		Path filepathurl = new Path(filepath);
		FSDataOutputStream fsdos = hdfs.create(filepathurl);
		IOUtils.copy(is, fsdos);
		fsdos.hflush();
		is.close();
		fsdos.close();
	}

	public static void loadProperties(String filename) throws Exception {
		Properties prop = new Properties();
		InputStream fis = MassiveDataMRJobBase.class.getResourceAsStream(filename);
		prop.load(fis);
		fis.close();
		if (MDCProperties.get() != null) {
			MDCProperties.get().putAll(prop);
		} else {
			MDCProperties.put(prop);
		}
	}

	@AfterClass
	public static void closeResources() throws Exception {
		executorpool.shutdown();
		for (HeartBeat hbss : hbssl) {
			hbss.stop();
			hbss.destroy();
		}
		testingserver.stop();
		testingserver.close();
	}
}
