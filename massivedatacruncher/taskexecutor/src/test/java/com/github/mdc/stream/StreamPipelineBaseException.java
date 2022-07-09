package com.github.mdc.stream;

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

import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.CacheUtils;
import com.github.mdc.common.HeartBeatServerStream;
import com.github.mdc.common.MDCCacheManager;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCExecutorThreadFactory;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Utils;
import com.github.mdc.tasks.executor.NodeRunner;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import com.github.sakserv.minicluster.impl.YarnLocalCluster;

public class StreamPipelineBaseException {
	static HdfsLocalCluster hdfsLocalCluster;
	String[] airlineheader = new String[]{"Year", "Month", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime",
			"ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime", "CRSElapsedTime",
			"AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut", "Cancelled",
			"CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay"};
	String[] carrierheader = {"Code", "Description"};
	static String hdfsfilepath = "hdfs://127.0.0.1:9000";
	String airlines = "/airlines";
	String airline = "/airline";
	static String airlinenoheader = "/airlinenoheader";
	static String airlinesamplenoheader = "/airlinesamplenoheader";
	String airlineverysmall = "/airlineverysmall";
	String airlineveryverysmall1988 = "/airlineververysmall1988";
	String airlineveryverysmall1989 = "/airlineveryverysmall1989";
	String airlinepartitionsmall = "/airlinepartitionsmall";
	String airlinesmall = "/airlinesmall";
	static String airlinesample = "/airlinesample";
	static String airlinesamplesql = "/airlinesamplesql";
	static String airlinesamplejoin = "/airlinesamplejoin";
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
	static int zookeeperport = 2182;
	static int namenodeport = 9000;
	static int namenodehttpport = 60070;
	public static final String ZK_BASE_PATH = "/mdc/cluster1";
	static private HeartBeatServerStream hb;
	static private String host;
	static Logger log = Logger.getLogger(StreamPipelineBaseException.class);
	static List<HeartBeatServerStream> hbssl = new ArrayList<>();
	static List<ServerSocket> sss = new ArrayList<>();
	static ExecutorService threadpool, executorpool;
	static int numberofnodes = 1;
	static Integer port;
	YarnLocalCluster yarnLocalCluster;
	static FileSystem hdfs;
	static boolean setupdone;
	static TestingServer testingserver;
	static ConcurrentMap<String, List<Process>> containerprocesses = new ConcurrentHashMap<>();
	static FileSystem hdfste;
	protected static PipelineConfig pipelineconfig = new PipelineConfig();

	@SuppressWarnings({"unused"})
	@BeforeClass
	public static void setServerUp() throws Exception {
		try {
			Output out = new Output(System.out);
			pipelineconfig.setKryoOutput(out);
			pipelineconfig.setMaxmem("1024");
			pipelineconfig.setMinmem("512");
			pipelineconfig.setGctype(MDCConstants.ZGC);
			pipelineconfig.setNumberofcontainers("3");
			pipelineconfig.setMode(MDCConstants.MODE_NORMAL);
			System.setProperty("HADOOP_HOME", "E:\\DEVELOPMENT\\hadoop\\hadoop-3.2.1");
			Utils.loadLog4JSystemPropertiesClassPath("mdctestexception.properties");
			CacheUtils.initCache();
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
				hb.init(rescheduledelay, port, host, initialdelay, pingdelay, "");
				hb.start();
				threadpool = Executors.newWorkStealingPool();
				executorpool = Executors.newWorkStealingPool();
				ClassLoader cl = Thread.currentThread().getContextClassLoader();
				port = Integer.parseInt(MDCProperties.get().getProperty("taskexecutor.port"));
				int executorsindex = 0;
				CountDownLatch cdl = new CountDownLatch(numberofnodes);
				CountDownLatch cdlport = new CountDownLatch(1);
				ConcurrentMap<String, Map<String, Process>> containerprocesses = new ConcurrentHashMap<>();
				ConcurrentMap<String, Map<String, List<Thread>>> containeridthreads = new ConcurrentHashMap<>();
				hdfste = FileSystem.get(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
						configuration);
				AtomicInteger portinc = new AtomicInteger(port);
				var containeridports = new ConcurrentHashMap<String, List<Integer>>();
				while (executorsindex < numberofnodes) {
					hb = new HeartBeatServerStream();
					host = NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty("taskexecutor.host"));
					hb.init(rescheduledelay, nodeport, host, initialdelay, pingdelay, "");
					hb.ping();
					hbssl.add(hb);
					executorpool.execute(() -> {
						ServerSocket server;
						try {
							server = new ServerSocket(nodeport, 256, InetAddress.getByAddress(new byte[]{0x00, 0x00, 0x00, 0x00}));
							sss.add(server);
							cdlport.countDown();
							cdl.countDown();
							while (true) {
								Socket client = server.accept();
								Future<Boolean> containerallocated = threadpool.submit(
										new NodeRunner(client, portinc, MDCConstants.TEPROPLOADCLASSPATHCONFIGEXCEPTION,
												containerprocesses, hdfste, containeridthreads, containeridports));
								log.info("Containers Allocated: " + containerallocated.get() + " Next Port Allocation:" + portinc.get());
							}
						} catch (Exception ioe) {
						}
					});
					cdlport.await();
					port += 100;
					executorsindex++;
				}
			}
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
		InputStream is = StreamPipelineBaseException.class.getResourceAsStream(filename);
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
		MDCCacheManager.get().close();
		MDCCacheManager.put(null);
		sss.stream().forEach(ss -> {
			try {
				ss.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
	}
}
