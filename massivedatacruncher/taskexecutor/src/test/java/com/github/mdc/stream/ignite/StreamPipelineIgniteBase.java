/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.mdc.stream.ignite;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.CacheUtils;
import com.github.mdc.common.HeartBeatStream;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Utils;
import com.github.mdc.common.utils.HadoopTestUtilities;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;
import com.github.sakserv.minicluster.impl.YarnLocalCluster;

public class StreamPipelineIgniteBase {
  static HdfsLocalCluster hdfsLocalCluster;
  String[] airlineheader = new String[] {"Year", "Month", "DayofMonth", "DayOfWeek", "DepTime",
      "CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum",
      "ActualElapsedTime", "CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest",
      "Distance", "TaxiIn", "TaxiOut", "Cancelled", "CancellationCode", "Diverted", "CarrierDelay",
      "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"};
  String[] carrierheader = {"Code", "Description"};
  static String hdfsfilepath = "hdfs://127.0.0.1:9100";
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
  static private HeartBeatStream hb;
  static Logger log = Logger.getLogger(StreamPipelineIgniteBase.class);
  static ExecutorService threadpool, executorpool;
  static int numberofnodes = 1;
  static Integer port;
  YarnLocalCluster yarnLocalCluster;
  static FileSystem hdfs;
  static ConcurrentMap<String, List<Process>> containerprocesses = new ConcurrentHashMap<>();
  protected static PipelineConfig pipelineconfig = new PipelineConfig();

  @SuppressWarnings({"unused"})
  @BeforeClass
  public static void setServerUp() throws Exception {
    org.burningwave.core.assembler.StaticComponentContainer.Modules.exportAllToAll();
    try {
      Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
          + MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, "mdctest.properties");
      ByteBufferPoolDirect.init();
      CacheUtils.initCache();
      CacheUtils.initBlockMetadataCache();
      try {
        hdfsLocalCluster = HadoopTestUtilities.initHdfsCluster(9100, 0, 1);
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
      } catch (Throwable e) {
      }
      Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
          + MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, "mdctest.properties");
      pipelineconfig.setLocal("false");
      pipelineconfig.setIsblocksuserdefined("false");
      pipelineconfig.setMode(MDCConstants.MODE_DEFAULT);
      Boolean ishdfs = Boolean.parseBoolean(MDCProperties.get().getProperty("taskexecutor.ishdfs"));
      Configuration configuration = new Configuration();
      hdfs = FileSystem.newInstance(
          new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)), configuration);
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
  }

  public static void uploadfile(FileSystem hdfs, String dir, String filename) throws Throwable {
    InputStream is = StreamPipelineIgniteBase.class.getResourceAsStream(filename);
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
    if (Objects.nonNull(hdfs)) {
      hdfs.close();
      hdfs = null;
    }
    if (hdfsLocalCluster != null) {
      hdfsLocalCluster.stop();
      hdfsLocalCluster.cleanUp();
      hdfsLocalCluster = null;
    }
    if (hb != null) {
      hb.close();
      hb = null;
    }
    if (executorpool != null) {
      executorpool.shutdown();
      executorpool = null;
    }
    if (threadpool != null) {
      threadpool.shutdown();
      threadpool = null;
    }
  }
}
