package com.github.mdc.stream;

import java.io.InputStream;
import java.net.InetAddress;
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
import java.util.concurrent.atomic.AtomicInteger;

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
import com.github.mdc.common.ByteBufferPool;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.HeartBeatServerStream;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.Utils;
import com.github.mdc.tasks.executor.NodeRunner;
import com.github.mdc.tasks.executor.MassiveDataCruncherMRApiTest;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	   StreamPipelineTestSuite2.class,
	   StreamPipelineTest.class,
	   StreamPipelineContinuedTest.class,
	   StreamPipelineFoldByKeyKeyByTest.class,
	   StreamPipelineTransformationsCollectTest.class,
	   StreamPipelineTransformationsNullTest.class,
	   StreamPipelineDepth2Test.class,
	   StreamPipelineDepth31Test.class,
	   StreamPipelineDepth32Test.class,
	   StreamPipelineDepth32ContinuedTest.class,
	   StreamPipelineDepth33Test.class,
	   StreamPipelineDepth34Test.class,
	   StreamPipelineUtilsTest.class,
	   StreamPipelineFunctionsTest.class,
	   StreamPipelineCoalesceTest.class,
	   StreamPipelineJsonTest.class,
	   StreamPipelineTransformationFunctionsTest.class,
	   HDFSBlockUtilsTest.class,
	   StreamPipelineStatisticsTest.class,
	   MassiveDataCruncherMRApiTest.class,
	   StreamPipelineSqlTest.class})
public class StreamPipelineTestSuite extends StreamPipelineBase {
	@SuppressWarnings({ "unused" })
	@BeforeClass
	public static void setServerUp() throws Exception {
		try {
			Utils.loadLog4JSystemPropertiesClassPath("mdctest.properties");
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
			ByteBufferPool.init(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.BYTEBUFFERPOOL_MAX, MDCConstants.BYTEBUFFERPOOL_MAX_DEFAULT)));
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
				int initialdelay = Integer.parseInt(MDCProperties.get().getProperty("taskschedulerstream.initialdelay"));
				int pingdelay = Integer.parseInt(MDCProperties.get().getProperty("taskschedulerstream.pingdelay"));
				host = NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty("taskschedulerstream.host"));
				port =  Integer.parseInt(MDCProperties.get().getProperty("taskschedulerstream.port"));
				int nodeport = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.NODE_PORT));
				hb.init(rescheduledelay, port, host, initialdelay, pingdelay,"");
				hb.start();
				threadpool = Executors.newWorkStealingPool();
				executorpool = Executors.newWorkStealingPool();
				ClassLoader cl = Thread.currentThread().getContextClassLoader();
				port = Integer.parseInt(MDCProperties.get().getProperty("taskexecutor.port"));
				int executorsindex = 0;
				CountDownLatch cdl = new CountDownLatch(numberofnodes);
				CountDownLatch cdlport = new CountDownLatch(1);
				ConcurrentMap<String, Map<String,Process>> containerprocesses = new ConcurrentHashMap<>();
				ConcurrentMap<String, Map<String,List<Thread>>> containeridthreads = new ConcurrentHashMap<>();
				hdfste = FileSystem.get(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
						configuration);
				var containeridports = new ConcurrentHashMap<String, List<Integer>>();
				while (executorsindex < numberofnodes) {
					hb = new HeartBeatServerStream();
					host = NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty("taskexecutor.host"));
					hb.init(rescheduledelay, nodeport, host, initialdelay, pingdelay,"");
					hb.ping();
					hbssl.add(hb);
					AtomicInteger portinc = new AtomicInteger(port);
					executorpool.execute(() -> {
						ServerSocket server;
						try {
							server = new ServerSocket(nodeport,256,InetAddress.getByAddress(new byte[] { 0x00, 0x00, 0x00, 0x00 }));
							sss.add(server);
							cdlport.countDown();
							cdl.countDown();
							while (true) {
								try(Socket sock = server.accept();) {
									var container = new NodeRunner(sock, portinc, MDCConstants.TEPROPLOADCLASSPATHCONFIG,
											containerprocesses, hdfs, containeridthreads,containeridports);
									Future<Boolean> containerallocated = threadpool.submit(container);
									log.info("Containers Allocated: "+containerallocated.get()+" Next Port Allocation:"+portinc.get());
								} catch (Exception e) {
								}
							}
						} catch (Exception ioe) {
						}
					});
					cdlport.await();
					port+=100;
					executorsindex++;
				}
			}
			try(Socket sock = new Socket("localhost",9000);){}
			catch(Exception ex) {
				Configuration conf = new Configuration();
				conf.set("fs.hdfs.impl.disable.cache", "false");
				conf.set("dfs.block.access.token.enable", "true");
				hdfsLocalCluster = new HdfsLocalCluster.Builder().setHdfsNamenodePort(namenodeport)
						.setHdfsNamenodeHttpPort(namenodehttpport).setHdfsTempDir("./target/embedded_hdfs")
						.setHdfsNumDatanodes(1).setHdfsEnablePermissions(false).setHdfsFormat(true)
						.setHdfsEnableRunningUserAsProxyUser(true).setHdfsConfig(conf).build();
	
				hdfsLocalCluster.start();
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
		if(!Objects.isNull(hdfste))hdfste.close();
		if(!Objects.isNull(hdfs))hdfs.close();
		if(hdfsLocalCluster!=null)hdfsLocalCluster.stop(true);
		if (hb != null)
			hb.close();
		if(executorpool!=null)
		executorpool.shutdown();
		if(threadpool!=null)
		threadpool.shutdown();
		testingserver.close();
		for (HeartBeatServerStream hbss : hbssl) {
			hbss.stop();
			hbss.destroy();
		}
		sss.stream().forEach(ss->{
			try {
				ss.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		ByteBufferPoolDirect.get().close();
	}
}
