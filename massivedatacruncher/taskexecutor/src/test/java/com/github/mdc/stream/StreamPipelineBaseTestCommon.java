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
package com.github.mdc.stream;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import java.io.InputStream;
import java.net.URI;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.IOUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.CacheUtils;
import com.github.mdc.common.HeartBeatStream;
import com.github.mdc.common.MDCCacheManager;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.StreamDataCruncher;
import com.github.mdc.common.TaskExecutorShutdown;
import com.github.mdc.common.Utils;
import com.github.mdc.common.utils.HadoopTestUtilities;
import com.github.mdc.tasks.executor.NodeRunner;

public class StreamPipelineBaseTestCommon extends StreamPipelineBase {
	static Registry server = null;
	static Logger log = Logger.getLogger(StreamPipelineBaseTestCommon.class);

	@SuppressWarnings({ "unused" })
	@BeforeClass
	public static void setServerUp() throws Exception {
		try {
			org.burningwave.core.assembler.StaticComponentContainer.Modules.exportAllToAll();
			Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
					+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, "mdctest.properties");
			var out = System.out;
			pipelineconfig.setOutput(out);
			pipelineconfig.setMaxmem("1024");
			pipelineconfig.setMinmem("512");
			pipelineconfig.setGctype(MDCConstants.ZGC);
			pipelineconfig.setNumberofcontainers("1");
			pipelineconfig.setMode(MDCConstants.MODE_NORMAL);
			pipelineconfig.setBatchsize("1");
			System.setProperty("HADOOP_HOME", "C:\\DEVELOPMENT\\hadoop\\hadoop-3.3.4");
			ByteBufferPoolDirect.init();
			CacheUtils.initCache();
			CacheUtils.initBlockMetadataCache();
			hdfsLocalCluster = HadoopTestUtilities.initHdfsCluster(9100, 9870, 2);
			pipelineconfig.setBlocksize("20");
			testingserver = new TestingServer(zookeeperport);
			testingserver.start();

			Boolean ishdfs = Boolean.parseBoolean(MDCProperties.get().getProperty("taskexecutor.ishdfs"));
			Configuration configuration = new Configuration();
			hdfs = FileSystem.newInstance(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
					configuration);
			Boolean islocal = Boolean.parseBoolean(pipelineconfig.getLocal());
			if (numberofnodes > 0) {
				hb = new HeartBeatStream();
				int rescheduledelay = Integer
						.parseInt(MDCProperties.get().getProperty("taskschedulerstream.rescheduledelay"));
				int initialdelay = Integer
						.parseInt(MDCProperties.get().getProperty("taskschedulerstream.initialdelay"));
				int pingdelay = Integer.parseInt(MDCProperties.get().getProperty("taskschedulerstream.pingdelay"));
				host = NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty("taskschedulerstream.host"));
				port = Integer.parseInt(MDCProperties.get().getProperty("taskschedulerstream.port"));
				int nodeport = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.NODE_PORT));
				hb.init(rescheduledelay, port, host, initialdelay, pingdelay, "");
				hb.start();
				threadpool = Executors.newSingleThreadExecutor();
				executorpool = Executors.newSingleThreadExecutor();
				ClassLoader cl = Thread.currentThread().getContextClassLoader();
				port = Integer.parseInt(MDCProperties.get().getProperty("taskexecutor.port"));
				int executorsindex = 0;
				CountDownLatch cdl = new CountDownLatch(numberofnodes);
				containerprocesses = new ConcurrentHashMap<>();
				ConcurrentMap<String, Map<String, List<Thread>>> containeridthreads = new ConcurrentHashMap<>();
				hdfste = FileSystem.get(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
						configuration);
				var containeridports = new ConcurrentHashMap<String, List<Integer>>();
				while (executorsindex < numberofnodes) {
					hb = new HeartBeatStream();
					host = NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty("taskexecutor.host"));
					hb.init(rescheduledelay, nodeport, host, initialdelay, pingdelay, "");
					hb.ping();
					hbssl.add(hb);
					if (isNull(server)) {
						server = Utils.getRPCRegistry(nodeport, new StreamDataCruncher() {
							public Object postObject(Object object) throws RemoteException {
								try {
									var container = new NodeRunner(MDCConstants.PROPLOADERCONFIGFOLDER,
											containerprocesses, hdfs, containeridthreads, containeridports, object);
									Future<Object> containerallocated = threadpool.submit(container);
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
						sss.add(server);
					}
					port += 100;
					executorsindex++;
				}
			}
			uploadfile(hdfs, airlinesamplecsv, airlinesamplecsv + csvfileextn);
			uploadfile(hdfs, airportssample, airportssample + csvfileextn);
			uploadfile(hdfs, airlinesample, airlinesample + csvfileextn);
			uploadfile(hdfs, airlinesamplesql, airlinesamplesql + csvfileextn);
			uploadfile(hdfs, airlinesamplejoin, airlinesamplejoin + csvfileextn);
			uploadfile(hdfs, carriers, carriers + csvfileextn);
			uploadfile(hdfs, airline1987, airline1987 + csvfileextn);
			uploadfile(hdfs, bicyclecrash, bicyclecrash + csvfileextn);
			uploadfile(hdfs, population, population + csvfileextn);
			uploadfile(hdfs, airlinepairjoin, airlinepairjoin + csvfileextn);
			uploadfile(hdfs, airlinenoheader, airlinenoheader + csvfileextn);
			uploadfile(hdfs, airlinesamplenoheader, airlinesamplenoheader + csvfileextn);
			uploadfile(hdfs, cars, cars + txtfileextn);
			uploadfile(hdfs, wordcount, wordcount + txtfileextn);
			uploadfile(hdfs, airlinemultiplefilesfolder, airlinesample + csvfileextn);
			uploadfile(hdfs, airlinemultiplefilesfolder, airlinenoheader + csvfileextn);
			uploadfile(hdfs, githubevents, githubevents + jsonfileextn);

		} catch (Throwable e) {
			log.info("Error Uploading file", e);
		}
		setupdone = true;
	}

	public static void uploadfile(FileSystem hdfs, String dir, String filename) throws Throwable {
		InputStream is = StreamPipelineBase.class.getResourceAsStream(filename);
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

	@AfterClass
	public static void closeResources() throws Exception {
		if(nonNull(MDCCacheManager.get())){
			MDCCacheManager.get().close();
			MDCCacheManager.put(null);
		}
		if (!Objects.isNull(hdfste)) {
			hdfste.close();
		}
		if (!Objects.isNull(hdfs)) {
			hdfs.close();
		}
		if (hdfsLocalCluster != null) {
			hdfsLocalCluster.stop(true);
		}
		if (hb != null) {
			hb.close();
		}
		if (executorpool != null) {
			executorpool.shutdown();
		}
		if (threadpool != null) {
			threadpool.shutdown();
		}
		testingserver.close();
		for (HeartBeatStream hbss : hbssl) {
			hbss.stop();
			hbss.destroy();
		}
		containerprocesses.keySet().stream().forEach(key -> {
			containerprocesses.get(key).keySet().stream().forEach(port -> {
				Process proc = containerprocesses.get(key).get(port);
				if (Objects.nonNull(proc)) {
					log.info("In DC else Destroying the Container Process: " + proc);
					try {
						TaskExecutorShutdown taskExecutorshutdown = new TaskExecutorShutdown();
						log.info("Destroying the TaskExecutor: "
								+ MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)
								+ MDCConstants.UNDERSCORE + port);
						Utils.getResultObjectByInput(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)
								+ MDCConstants.UNDERSCORE + port, taskExecutorshutdown);
						log.info("Checking the Process is Alive for: "
								+ MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)
								+ MDCConstants.UNDERSCORE + port);
						while (proc.isAlive()) {
							log.info("Destroying the TaskExecutor: "
									+ MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)
									+ MDCConstants.UNDERSCORE + port);
							Thread.sleep(500);
						}
						log.info("Process Destroyed: " + proc + " for the port " + port);
					} catch (Exception ex) {
						log.error("Destroy failed for the process: " + proc);
					}
				}
			});
		});
		ByteBufferPoolDirect.destroy();
	}
}
