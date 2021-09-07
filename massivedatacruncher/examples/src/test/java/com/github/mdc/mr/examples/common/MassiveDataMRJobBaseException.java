package com.github.mdc.mr.examples.common;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.github.mdc.common.CacheUtils;
import com.github.mdc.common.HeartBeatServer;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCExecutorThreadFactory;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.Utils;
import com.github.mdc.tasks.executor.Container;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;

public class MassiveDataMRJobBaseException {

	static Logger log = Logger.getLogger(MassiveDataMRJobBaseException.class);

	static HdfsLocalCluster hdfsLocalCluster;
	static int namenodeport = 9000;
	static int namenodehttpport = 50070;
	static FileSystem hdfs;
	String[] carrierheader = { "Code", "Description" };
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
	static List<HeartBeatServer> hbssl = new ArrayList<>();
	static ExecutorService executorpool;
	static List<ServerSocket> ssl = new ArrayList<>();
	static int zookeeperport = 2182;

	private static TestingServer testingserver;

	private static FileSystem hdfste;
	@BeforeClass
	public static void setServerUp() throws Exception {
		try {
			System.setProperty("HIBCFG", "../config/mdchibernate.cfg.xml");
			System.setProperty("HADOOP_HOME", "D:\\DEVELOPMENT\\hadoop-2.7.1");
			Utils.loadLog4JSystemPropertiesClassPath("mdctestexception.properties");
			CacheUtils.initCache();
			testingserver = new TestingServer(zookeeperport);
			testingserver.start();
			executorpool = Executors.newFixedThreadPool(3, new MDCExecutorThreadFactory(10));
			int rescheduledelay = Integer.parseInt(MDCProperties.get().getProperty("taskscheduler.rescheduledelay"));
			int initialdelay = Integer.parseInt(MDCProperties.get().getProperty("taskscheduler.initialdelay"));
			int pingdelay = Integer.parseInt(MDCProperties.get().getProperty("taskscheduler.pingdelay"));
			String host = NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty("taskscheduler.host"));
			port = Integer.parseInt(MDCProperties.get().getProperty("taskscheduler.port"));
			HeartBeatServer hb = new HeartBeatServer();
			hb.init(rescheduledelay, port, host, initialdelay, pingdelay, "");
			hb.start();
			try (Socket sock = new Socket(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST), 9000);) {
			} catch (Exception ex) {
				Configuration conf = new Configuration();
				conf.set("fs.hdfs.impl.disable.cache", "false");
				conf.set("dfs.block.access.token.enable", "true");
				hdfsLocalCluster = new HdfsLocalCluster.Builder().setHdfsNamenodePort(namenodeport)
						.setHdfsNamenodeHttpPort(namenodehttpport).setHdfsTempDir("./target/embedded_hdfs")
						.setHdfsNumDatanodes(1).setHdfsEnablePermissions(false).setHdfsFormat(true)
						.setHdfsEnableRunningUserAsProxyUser(true).setHdfsConfig(conf).build();

				hdfsLocalCluster.start();
			}
			Configuration configuration = new Configuration();
			hdfs = FileSystem.get(new URI(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HDFSNN)),
					configuration);
			log.info("HDFS FileSystem Object: "+hdfs);
			if (numberofnodes > 0) {
				port = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.NODE_PORT));
				int executorsindex = 0;
				ConcurrentMap<String, Map<String,Process>> containerprocesses = new ConcurrentHashMap<>();
				ConcurrentMap<String, Map<String,List<Thread>>> containeridthreads = new ConcurrentHashMap<>();
				ExecutorService es = Executors.newFixedThreadPool(100);
				CountDownLatch cdl = new CountDownLatch(numberofnodes);
				CountDownLatch cdlport = new CountDownLatch(1);
				hdfste = FileSystem.get(new URI(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HDFSNN)),
						configuration);
				AtomicInteger portinc = new AtomicInteger(port);
				var containeridports = new ConcurrentHashMap<String, List<Integer>>();
				while (executorsindex < numberofnodes) {
					hb = new HeartBeatServer();
					host = NetworkUtil
							.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST));
					hb.init(rescheduledelay, port, host, initialdelay, pingdelay, "");
					hb.ping();
					hbssl.add(hb);
					int teport = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PORT));
					executorpool.execute(() -> {
						ServerSocket server;
						try {
							server = new ServerSocket(port,256,InetAddress.getByAddress(new byte[] { 0x00, 0x00, 0x00, 0x00 }));
							cdlport.countDown();
							cdl.countDown();
							while (true) {
								Socket client = server.accept();
								Future<Boolean> containerallocated = es.submit(
										new Container(client, portinc, MDCConstants.TEPROPLOADCLASSPATHCONFIGEXCEPTION,
												containerprocesses, hdfste, containeridthreads, containeridports));
								log.info("Containers Allocated: "+containerallocated.get()+" Next Port Allocation:"+portinc.get());
							}
						} catch (Exception ioe) {
							log.info(MDCConstants.EMPTY,ioe);
						}
					});
					cdlport.await();
					port++;
					executorsindex++;
				}
				cdl.await();
			}
			
			uploadfile(hdfs, airlinesample, airlinesample + csvfileextn);
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
		} catch (Exception ex) {
			log.info("MRJobTestBase Initialization Error", ex);
		}
	}

	

	public static void uploadfile(FileSystem hdfs, String dir, String filename) throws Exception {
		InputStream is = MassiveDataMRJobBaseException.class.getResourceAsStream(filename);
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
		executorpool.shutdown();
		if(!Objects.isNull(hdfste)) {
			hdfste.close();
		}
		for (HeartBeatServer hbss : hbssl) {
			hbss.stop();
			hbss.destroy();
		}
		for (ServerSocket ss : ssl) {
			ss.close();
		}
		testingserver.stop();
		testingserver.close();
	}
}