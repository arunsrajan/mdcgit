package com.github.mdc.stream.ignite;

import java.io.InputStream;
import java.net.Socket;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.HeartBeatServerStream;
import com.github.mdc.common.MDCCacheManager;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Utils;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import com.github.sakserv.minicluster.impl.YarnLocalCluster;

public class MassiveDataPipelineIgniteBase {
	static HdfsLocalCluster hdfsLocalCluster;
	String[] airlineheader = new String[] { "Year", "Month", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime",
			"ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime", "CRSElapsedTime",
			"AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut", "Cancelled",
			"CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay" };
	String[] carrierheader = { "Code", "Description" };
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
	static Logger log = Logger.getLogger(MassiveDataPipelineIgniteBase.class);
	static ExecutorService threadpool, executorpool;
	static int numberofnodes = 1;
	static Integer port;
	YarnLocalCluster yarnLocalCluster = null;
	static FileSystem hdfs;
	static boolean setupdone = false;
	static ConcurrentMap<String, List<Process>> containerprocesses = new ConcurrentHashMap<>();
	protected static PipelineConfig pipelineconfig = new PipelineConfig();

	@SuppressWarnings({ "unused" })
	@BeforeClass
	public static void setServerUp() throws Exception {
		try {
			Utils.loadLog4JSystemPropertiesClassPath("mdctest.properties");
			Output out = new Output(System.out);
			pipelineconfig.setKryoOutput(out);
			pipelineconfig.setLocal("false");
			pipelineconfig.setIsblocksuserdefined("false");
			pipelineconfig.setMode(MDCConstants.MODE_DEFAULT);
			try (Socket sock = new Socket("localhost", 9000);) {
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
			Boolean ishdfs = Boolean.parseBoolean(MDCProperties.get().getProperty("taskexecutor.ishdfs"));
			Configuration configuration = new Configuration();
			hdfs = FileSystem.newInstance(new URI(MDCProperties.get().getProperty("taskexecutor.hdfsnn")),
					configuration);
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
		InputStream is = MassiveDataPipelineIgniteBase.class.getResourceAsStream(filename);
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
		hdfs.close();
		if (hdfsLocalCluster != null)
			hdfsLocalCluster.stop(true);
		if (hb != null)
			hb.close();
		if (executorpool != null)
			executorpool.shutdown();
		if (threadpool != null)
			threadpool.shutdown();
		if (!Objects.isNull(MDCCacheManager.get())) {
			MDCCacheManager.get().close();
			MDCCacheManager.put(null);
		}

	}
}
