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

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.github.mdc.common.*;
import org.apache.commons.io.IOUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.tasks.executor.MassiveDataCruncherMRApiTest;
import com.github.mdc.tasks.executor.NodeRunner;

@RunWith(Suite.class)
@Suite.SuiteClasses({ StreamPipelineTestSuite2.class, StreamPipelineTest.class, StreamPipelineContinuedTest.class,
		StreamPipelineFoldByKeyKeyByTest.class, StreamPipelineTransformationsCollectTest.class,
		StreamPipelineTransformationsNullTest.class, StreamPipelineDepth2Test.class, StreamPipelineDepth31Test.class,
		StreamPipelineDepth32Test.class, StreamPipelineDepth32ContinuedTest.class, StreamPipelineDepth33Test.class,
		StreamPipelineDepth34Test.class, StreamPipelineUtilsTest.class, StreamPipelineFunctionsTest.class,
		StreamPipelineCoalesceTest.class, StreamPipelineJsonTest.class, StreamPipelineTransformationFunctionsTest.class,
		HDFSBlockUtilsTest.class, StreamPipelineStatisticsTest.class, MassiveDataCruncherMRApiTest.class,
		StreamPipelineSqlTest.class })
public class StreamPipelineTestSuite extends StreamPipelineBase {
	@SuppressWarnings({ "unused" })
	@BeforeClass
	public static void setServerUp() throws Exception {
		try {
			Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
					+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, "mdctest.properties");
			Output out = new Output(System.out);
			pipelineconfig.setKryoOutput(out);
			pipelineconfig.setMaxmem("1024");
			pipelineconfig.setMinmem("512");
			pipelineconfig.setGctype(MDCConstants.ZGC);
			pipelineconfig.setNumberofcontainers("1");
			pipelineconfig.setMode(MDCConstants.MODE_NORMAL);
			pipelineconfig.setBatchsize("1");
			System.setProperty("HADOOP_HOME", "C:\\DEVELOPMENT\\hadoop\\hadoop-3.3.1");
			ByteBufferPoolDirect.init();
			ByteBufferPool.init(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.BYTEBUFFERPOOL_MAX,
					MDCConstants.BYTEBUFFERPOOL_MAX_DEFAULT)));
			pipelineconfig.setBlocksize("20");
			testingserver = new TestingServer(zookeeperport);
			testingserver.start();

			Boolean ishdfs = Boolean.parseBoolean(MDCProperties.get().getProperty("taskexecutor.ishdfs"));
			Configuration configuration = new Configuration();
			hdfs = FileSystem.newInstance(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
					configuration);
			Boolean islocal = Boolean.parseBoolean(pipelineconfig.getLocal());
			if (numberofnodes > 0) {
				hb = new HeartBeatServerStream();
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
				CountDownLatch cdlport = new CountDownLatch(1);
				ConcurrentMap<String, Map<String, Process>> containerprocesses = new ConcurrentHashMap<>();
				ConcurrentMap<String, Map<String, List<Thread>>> containeridthreads = new ConcurrentHashMap<>();
				hdfste = FileSystem.get(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
						configuration);
				var containeridports = new ConcurrentHashMap<String, List<TaskExecutorPorts>>();
				while (executorsindex < numberofnodes) {
					hb = new HeartBeatServerStream();
					host = NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty("taskexecutor.host"));
					hb.init(rescheduledelay, nodeport, host, initialdelay, pingdelay, "");
					hb.ping();
					hbssl.add(hb);
					executorpool.execute(() -> {
						ServerSocket server;
						try {
							server = Utils.createSSLServerSocket(nodeport);
							sss.add(server);
							cdlport.countDown();
							cdl.countDown();
							while (true) {
								try (Socket sock = server.accept();) {
									var container = new NodeRunner(sock, MDCConstants.TEPROPLOADDISTROCONFIG,
											containerprocesses, hdfs, containeridthreads, containeridports);
									Future<Boolean> containerallocated = threadpool.submit(container);
									log.info("Containers Allocated: " + containerallocated.get());
								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						} catch (Exception ioe) {
							ioe.printStackTrace();
						}
					});
					cdlport.await();
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
		for (HeartBeatServerStream hbss : hbssl) {
			hbss.stop();
			hbss.destroy();
		}
		sss.stream().forEach(ss -> {
			try {
				ss.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		ByteBufferPoolDirect.get().close();
	}
}
