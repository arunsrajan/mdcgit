package com.github.mdc.stream;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import com.github.mdc.common.HeartBeatServerStream;
import com.github.mdc.common.PipelineConfig;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import com.github.sakserv.minicluster.impl.YarnLocalCluster;

public class StreamPipelineBase {
	static HdfsLocalCluster hdfsLocalCluster;
	String[] airlineheader = new String[] { "Year", "Month", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime",
			"ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime", "CRSElapsedTime",
			"AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut", "Cancelled",
			"CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay" };
	String[] carrierheader = { "Code", "Description" };
	static SqlTypeName[] airsqltype = {SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,
			SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR
			,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,
			SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,
			SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR};
	static String[] airportsheader = {"iata","airport","city","state","country","latitude","longitude"};
	static SqlTypeName[] airportstype = {SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,
			SqlTypeName.VARCHAR,SqlTypeName.VARCHAR,SqlTypeName.VARCHAR};
	protected static String hdfsfilepath = "hdfs://127.0.0.1:9000";
	protected static String airlines = "/airlines";
	String airline = "/airline";
	protected String airline1989 = "/airline1989";
	static String airlinenoheader = "/airlinenoheader";
	static String airlinesamplenoheader = "/airlinesamplenoheader";
	String airlineverysmall = "/airlineverysmall";
	String airlineveryverysmall1988 = "/airlineververysmall1988";
	String airlineveryverysmall1989 = "/airlineveryverysmall1989";
	String airlinepartitionsmall = "/airlinepartitionsmall";
	String airlinesmall = "/airlinesmall";
	protected static String airlinesample = "/airlinesample";
	static String airportssample = "/airports";
	static String airlinesamplesql = "/airlinesamplesql";
	static String airlinesamplejoin = "/airlinesamplejoin";
	static String airlinesamplecsv = "/airlinesamplecsv";
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
	static SqlTypeName[] carriersqltype = {SqlTypeName.VARCHAR,SqlTypeName.VARCHAR};
	static String cars = "/cars";
	String groupbykey = "/groupbykey";
	static String bicyclecrash = "/bicyclecrash";
	static String airlinemultiplefilesfolder = "/airlinemultiplefilesfolder";
	static String githubevents = "/githubevents";
	static int zookeeperport = 2182;
	static int namenodeport = 9000;
	static int namenodehttpport = 60070;
	public static final String ZK_BASE_PATH = "/mdc/cluster1";
	protected static HeartBeatServerStream hb;
	protected static String host;
	static Logger log = Logger.getLogger(StreamPipelineBase.class);
	static List<HeartBeatServerStream> hbssl = new ArrayList<>();
	static List<ServerSocket> sss = new ArrayList<>();
	static ExecutorService threadpool, executorpool;
	static int numberofnodes = 1;
	static Integer port;
	YarnLocalCluster yarnLocalCluster = null;
	protected static FileSystem hdfs;
	static boolean setupdone = false,toteardownclass = false;
	static TestingServer testingserver;
	static ConcurrentMap<String, List<Process>> containerprocesses = new ConcurrentHashMap<>();
	static FileSystem hdfste;
	protected static PipelineConfig pipelineconfig = new PipelineConfig();

	
}
