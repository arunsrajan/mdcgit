package com.github.mdc.stream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.ehcache.Cache;

import com.github.sakserv.minicluster.impl.HdfsLocalCluster;

public class MDCPipelineTestsCommon {
	public static Cache<String,byte[]> cache = null;
	protected static String STAR = "*";
	protected static int namenodeport = 9000;
	protected static int namenodehttpport = 50070;
	protected static HdfsLocalCluster hdfsLocalCluster;
	protected static FileSystem hdfs;
	protected static String airlinesample = "/airlinesample";
	protected static String airlinesampleintersection = "/airlinesampleintersection";
	protected static String airlinesampleunion1 = "/airlinesampleunion1";
	protected static String airlinesampleunion2 = "/airlinesampleunion2";
	protected static String githubevents = "/githubevents";
	protected static String csvfileextn = ".csv";
	protected String[] hdfsdirpaths1 = {"/airlinesample"};
	protected String[] hdfsdirpaths2 = {"/airlinesampleintersection"};
	protected String[] hdfsdirpaths3 = {"/airlinesampleunion1"};
	protected String[] hdfsdirpaths4 = {"/airlinesampleunion2"};
	protected String[] githubevents1 = {"/githubevents"};
	protected static String jsonfileextn = ".json";
	protected  static String hdfsurl = "hdfs://127.0.0.1:9000";
	protected String[] airlineheader = new String[] { "Year", "Month", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime",
			"ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime", "CRSElapsedTime",
			"AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut", "Cancelled",
			"CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay" };
	static Logger log = Logger.getLogger(MDCPipelineTestsCommon.class);
	
}
