package com.github.mdc.tasks.scheduler;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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

import com.github.mdc.common.ByteBufferPool;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.CacheUtils;
import com.github.mdc.common.HeartBeatServer;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
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
	static List<HeartBeatServer> hbssl = new ArrayList<>();
	static ExecutorService executorpool;
	static List<ServerSocket> ssl = new ArrayList<>();
	static int zookeeperport = 2182;

	private static TestingServer testingserver;

	@BeforeClass
	public static void setServerUp() throws Exception {
		try (InputStream istream = MassiveDataMRJobBase.class.getResourceAsStream("/log4j.properties");) {
			System.setProperty("HIBCFG", "../config/mdchibernate.cfg.xml");
			System.setProperty("HADOOP_HOME", "C:\\DEVELOPMENT\\hadoop\\hadooplocal\\hadoop-3.3.1");
			PropertyConfigurator.configure(istream);
			Utils.loadLog4JSystemPropertiesClassPath("mdctest.properties");
			ByteBufferPoolDirect.init();
			ByteBufferPool.init(4);
			CacheUtils.initCache();
			testingserver = new TestingServer(zookeeperport);
			testingserver.start();
			executorpool = Executors.newWorkStealingPool();
			int rescheduledelay = Integer.parseInt(MDCProperties.get().getProperty("taskscheduler.rescheduledelay"));
			int initialdelay = Integer.parseInt(MDCProperties.get().getProperty("taskscheduler.initialdelay"));
			int pingdelay = Integer.parseInt(MDCProperties.get().getProperty("taskscheduler.pingdelay"));
			String host = NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty("taskscheduler.host"));
			port = Integer.parseInt(MDCProperties.get().getProperty("taskscheduler.port"));
			HeartBeatServer hb = new HeartBeatServer();
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
				CountDownLatch cdl = new CountDownLatch(numberofnodes);
				CountDownLatch cdlport = new CountDownLatch(1);
				var containeridports = new ConcurrentHashMap<String, List<Integer>>();
				while (executorsindex < numberofnodes) {
					hb = new HeartBeatServer();
					host = NetworkUtil
							.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST));
					hb.init(rescheduledelay, port, host, initialdelay, pingdelay, "");
					hb.ping();
					hbssl.add(hb);
					Thread.sleep(3000);
					int teport = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PORT));
					AtomicInteger portinc = new AtomicInteger(teport);
					executorpool.execute(() -> {
						ServerSocket server;
						try {
							server = new ServerSocket(port, 256, InetAddress.getByAddress(new byte[]{0x00, 0x00, 0x00, 0x00}));
							cdlport.countDown();
							cdl.countDown();
							while (true) {
								Socket client = server.accept();
								es.submit(
										new NodeRunner(client, portinc, MDCConstants.TEPROPLOADCLASSPATHCONFIG,
												containerprocesses, hdfs, containeridthreads, containeridports));
							}
						} catch (Exception ioe) {
							log.info(MDCConstants.EMPTY, ioe);
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
