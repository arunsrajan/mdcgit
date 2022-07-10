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
package com.github.mdc.common;

/**
 * 
 * @author Arun 
 * Holds the required constants for the task schedulers, executors,
 * Data replicator, Yarn and mesos schedulers and exectors.
 */
public class MDCConstants {
	private MDCConstants() {
	}
	static final String MDC = "MDC";
	public static final String NEWLINE = "\n";
	public static final String HYPHEN = "-";
	static final String SLASH = "\\";
	public static final String FORWARD_SLASH = "/";
	public static final String BACKWARD_SLASH = "\\";
	public static final String COLON = ":";
	public static final String EQUAL = "=";
	public static final String UNDERSCORE = "_";
	public static final String EMPTY = "";
	static final char DOT = '.';
	public static final String ASTERIX = "*";
	public static final String COMMA = ",";
	public static final String SINGLE_QUOTES = "'";
	public static final String SINGLESPACE = " ";
	static final String CLASS = "class";
	public static final String NULL = "null";
	public static final String NULLSTRING = null;

	public static final String PROPERTIESEXTN = ".properties";

	static final String LOCALHOST = "localhost";
	public static final String TASKEXECUTOR = "taskexecutor";
	static final String ZKOPERATION_CREATED = " created";
	public static final String META_INF = "META-INF";
	public static final String MANIFEST_MF = "MANIFEST.MF";
	public static final String MAIN_CLASS = "Main-Class";
	public static final String RUNMRJOB_METHOD = "runMRJob";
	public static final String RUNPIPELINE_METHOD = "runPipeline";

	public static final String LOCAL_FS_APPJRPATH = "../appjar/";
	public static final String DIST_CONFIG_FOLDER = "config";
	public static final String PREV_FOLDER = "..";
	public static final String SPARSE = "Sparse";
	public static final String NORMAL = "Normal";
	public static final String NODES = "nodes";
	public static final String LOG4J_PROPERTIES = "log4j.properties";
	public static final String TASKSCHEDULERSTREAM = "taskschedulerstream";
	public static final String TASKSCHEDULER = "taskscheduler";
	public static final String TASKSCHEDULER_HOSTPORT = "taskscheduler.hostport";
	public static final String METAFILEEXTN = ".meta";
	public static final String MDCAPPLICATION = "App";
	public static final String BLOCK = "Block";
	public static final String TASK = "Task";
	public static final String CONTAINER = "Container";
	public static final String MAPRED = "MapRed";
	public static final String CHUNK = "chunk";
	public static final String SHARD = "shard";
	public static final String REPLICA = "replica";
	public static final String MD5 = "MD5";
	public static final String CPZ = "cpz";
	public static final String LEADER = "leader";
	public static final String TSS = "tss";
	public static final String TE = "te";
	public static final String TS = "ts";
	public static final String DR = "dr";
	public static final String DM = "dm";
	public static final String JOB = "Job";
	public static final String STAGE = "Stage";
	public static final String DMMETA = "DMMeta";
	public static final String PENDINGJOBS = "PENDINGJOBS";
	public static final String RUNNINGJOBS = "RUNNINGJOBS";
	static final String HEARTBEAT_EXCEPTION_MESSAGE = "HeartBeat [group,port,delay] and DataReplicator [serverport,host] must be provided";
	static final String HEARTBEAT_EXCEPTION_PING_DELAY = "arg 5 must be proper ping delay of type Integer";
	static final String HEARTBEAT_EXCEPTION_CONTAINER_ID = "arg 6 must be proper containerid of type String";
	static final String HEARTBEAT_EXCEPTION_SERVER_PORT = "arg 2 must be proper server port of type Integer";
	static final String HEARTBEAT_EXCEPTION_SERVER_HOST = "arg 3 must be proper server host address of type String";
	static final String HEARTBEAT_EXCEPTION_RESCHEDULE_DELAY = "arg 1 must be proper ping delay of type Integer";
	static final String HEARTBEAT_EXCEPTION_INITIAL_DELAY = "arg 4 must be proper ping delay of type Integer";
	static final String HEARTBEAT_TASK_SCHEDULER_EXCEPTION_MESSAGE = "HeartBeatTaskScheduler [group,port,delay,serverport,host,applicationid,taskid] must be provided";
	static final String HEARTBEAT_TASK_SCHEDULER_EXCEPTION_APPID = "arg[6] must be proper applicationid of type String";
	static final String HEARTBEAT_TASK_SCHEDULER_EXCEPTION_TASKID = "arg[7] must be proper taskid of type String";
	static final String HEARTBEAT_TASK_SCHEDULER_STREAM_EXCEPTON_MESSAGE = "HeartBeattaskschedulerstreamStream [group,port,delay,serverport,host,jobid,stageid] must be provided";
	static final String HEARTBEAT_TASK_SCHEDULER_STREAM_EXCEPTON_JOBID = "arg[7] must be proper jobid of type String";
	static final String HEARTBEAT_TASK_SCHEDULER_STREAM_EXCEPTON_CURR_STAGE_INDEX = "arg[8] must be proper currentstageindex of type Integer";
	public static final String DATABLOCK_EXCEPTION = "Unable to get Next Located Block";
	public static final String TSEXCEEDEDEXECUTIONCOUNT = "Tasks Execution Count Exceeded (Max Execution Count = 3)";

	public static final String TASKSCHEDULER_INITIALDELAY = "taskscheduler.initialdelay";
	public static final String TASKSCHEDULER_RESCHEDULEDELAY = "taskscheduler.rescheduledelay";
	public static final String TASKSCHEDULER_PINGDELAY = "taskscheduler.pingdelay";
	public static final String TASKSCHEDULER_HOST = "taskscheduler.host";
	public static final String TASKSCHEDULER_PORT = "taskscheduler.port";
	public static final String TASKSCHEDULER_ISHDFS = "taskscheduler.ishdfs";
	public static final String TASKSCHEDULER_BLOCKSIZE = "taskscheduler.blocksize";
	public static final String TASKSCHEDULER_BATCHSIZE = "taskscheduler.batchsize";
	public static final String TASKSCHEDULER_NUMREDUCERS = "taskscheduler.numreducers";
	public static final String TASKSCHEDULER_TMP_DIR = "taskscheduler.temp.dir";
	public static final String TASKSCHEDULERSTREAM_INITIALDELAY = "taskschedulerstream.initialdelay";
	public static final String TASKSCHEDULERSTREAM_INITIALDELAY_DEFAULT = "1000";
	public static final String TASKSCHEDULERSTREAM_RESCHEDULEDELAY = "taskschedulerstream.rescheduledelay";
	public static final String TASKSCHEDULERSTREAM_RESCHEDULEDELAY_DEFAULT = "10000";
	public static final String TASKSCHEDULERSTREAM_ISMESOS = "taskschedulerstream.ismesos";
	public static final String TASKSCHEDULERSTREAM_ISMESOS_DEFAULT = "false";
	public static final String TASKSCHEDULERSTREAM_ISYARN = "taskschedulerstream.isyarn";
	public static final String TASKSCHEDULERSTREAM_ISYARN_DEFAULT = "false";
	public static final String TASKSCHEDULERSTREAM_ISLOCAL = "taskschedulerstream.islocal";
	public static final String TASKSCHEDULERSTREAM_ISLOCAL_DEFAULT = "false";
	public static final String TASKSCHEDULERSTREAM_ISJGROUPS = "taskschedulerstream.isjgroups";
	public static final String TASKSCHEDULERSTREAM_ISJGROUPS_DEFAULT = "false";
	public static final String TASKSCHEDULERSTREAM_PINGDELAY = "taskschedulerstream.pingdelay";
	public static final String TASKSCHEDULERSTREAM_PINGDELAY_DEFAULT = "1000";
	public static final String TASKSCHEDULERSTREAM_PORT = "taskschedulerstream.port";
	public static final String TASKSCHEDULERSTREAM_HOST = "taskschedulerstream.host";
	public static final String TASKSCHEDULERSTREAM_HOSTPORT = "taskschedulerstream.hostport";
	public static final String TASKSCHEDULERSTREAM_BATCHSIZE = "taskschedulerstream.batchsize";
	public static final String TASKSCHEDULERSTREAM_BATCHSIZE_DEFAULT = "1";
	public static final String TASKSCHEDULERSTREAM_BLOCKSIZE = "taskschedulerstream.blocksize";
	public static final String TASKSCHEDULERSTREAM_BLOCKSIZE_DEFAULT = "64";
	public static final String TASKSCHEDULERSTREAM_HA_ENABLED = "taskschedulerstream.ha.enabled";
	public static final String TASKSCHEDULERSTREAM_HA_ENABLED_DEFAULT = "false";
	public static final String TASKEXECUTOR_PINGDELAY = "taskexecutor.pingdelay";
	public static final String TASKEXECUTOR_PORT = "taskexecutor.port";
	public static final String TASKEXECUTOR_HOST = "taskexecutor.host";
	public static final String NUMOFTASKSTATUSCOMPLETEDORFAILED = "jgroups.numtaskstatuscompleted";

	//Node Properties
	public static final String NODE_PORT = "node.port";
	public static final String CONTAINER_ALLOC = "container.alloc";
	public static final String CONTAINER_ALLOC_DEFAULT = "COMBINE";
	public static final String CONTAINER_ALLOC_COMBINE = "COMBINE";
	public static final String CONTAINER_ALLOC_DIVIDED = "DIVIDED";
	public static final String CONTAINER_ALLOC_IMPLICIT = "IMPLICIT";

	public static final String ZOOKEEPER_HOSTPORT = "zookeeper.hostport";
	public static final String ZOOKEEPER_RETRYDELAY = "zookeeper.retrydelay";
	public static final String ZOOKEEPER_STANDALONE_CLIENTPORT = "zookeeper.standalone.clientport";
	public static final String ZOOKEEPER_STANDALONE_NUMCONNECTIONS = "zookeeper.standalone.numconnections";
	public static final String ZOOKEEPER_STANDALONE_TICKTIME = "zookeeper.standalone.ticktime";
	public static final String ZOOKEEPER_STANDALONE_CLIENTPORT_DEFAULT = "2181";
	public static final String ZOOKEEPER_STANDALONE_NUMCONNECTIONS_DEFAULT = "200";
	public static final String ZOOKEEPER_STANDALONE_TICKTIME_DEFAULT = "200";
	static final String JOBSTAGE = "JobStage";
	static final String JOBPOOL = "JobPool";
	static final String THREAD = "Thread";
	public static final String TASKEXECUTOR_PROCESSORS = "taskexecutor.numprocessors";
	public static final String TASKSCHEDULER_RANDTE = "taskscheduler.randomte";
	public static final String TASKSCHEDULER_RANDTE_DEFAULT = "false";
	public static final String MINMEMORY = "container.minmem";
	public static final String MAXMEMORY = "container.maxmem";
	public static final String GCCONFIG = "container.gcconfig";
	public static final String NUMBEROFCONTAINERS = "containers.number";
	public static final String CONTAINER_MINMEMORY_DEFAULT = "1024";
	public static final String CONTAINER_MAXMEMORY_DEFAULT = "1024";
	public static final String GCCONFIG_DEFAULT = "-XX:+UseG1GC";
	public static final String NUMBEROFCONTAINERS_DEFAULT = "1";
	public static final String ISUSERDEFINEDBLOCKSIZE_DEFAULT = "false";

	static final String HDFS_DEFAULTFS = "fs.defaultFS";
	static final String HDFS_IMPL = "fs.hdfs.impl";
	static final String HDFS_FILE_IMPL = "fs.file.impl";
	public static final String HDFS_PROTOCOL = "hdfs";
	public static final String FILE_PROTOCOL = "file";

	public static final String ZK_BASE_PATH = "/mdc/cluster1";

	public static final String MDC_PROPERTIES = "mdc.properties";
	public static final String MDC_TEST_PROPERTIES = "mdctest.properties";
	public static final String MDC_TEST_EXCEPTION_PROPERTIES = "mdctestexception.properties";

	public static final String MESOS_FRAMEWORK_NAME = "MassiveDataCruncher";
	public static final String MESOS_FRAMEWORK_EXECUTOR_NAME = "MassiveDataCruncherExecutor";
	public static final String MESOS_FRAMEWORK_SHADED_JAR_PATH = "/opt/mdc/mesos/MassiveDataStream-1.0.0-shaded.jar";
	public static final String MESOS_FRAMEWORK_TASK_EXECUTOR_COMMAND = "exec /usr/local/java/jdk-15.0.1/bin/java -Xms3G -Xmx3G -Djava.library.path=/usr/lib64/ -classpath \".:MassiveDataStream-1.0.0-shaded.jar\" com.github.mdc.stream.mesos.executor.MesosExecutor";
	public static final String MESOS_MASTER = "taskschedulerstream.mesosmaster";
	public static final String MESOS_MASTER_DEFAULT = "localhost:5050";
	public static final String MESOS_CHECKPOINT = "MESOS_CHECKPOINT";
	public static final String MESOS_AUTHENTICATE = "MESOS_AUTHENTICATE";
	public static final String DEFAULT_PRINCIPAL = "DEFAULT_PRINCIPAL";
	public static final String DEFAULT_SECRET = "DEFAULT_SECRET";
	public static final String MESOS_TASK = "task ";
	public static final String CPUS = "cpus";
	public static final String MEM = "mem";

	public static final String MESOS_CONFIGDIR = "../config/";

	public static final String YARNINPUTFOLDER = "yarninput";
	public static final String MASSIVEDATA_YARNINPUT_DATAFILE = "mdststs.dat";
	public static final String MASSIVEDATA_YARNINPUT_GRAPH_FILE = "graph.dat";
	public static final String MASSIVEDATA_YARNINPUT_TASK_FILE = "taskmdsthread.dat";
	public static final String MASSIVEDATA_YARNINPUT_JOBSTAGE_FILE = "jobstagemapfile.dat";
	public static final String YARN_CLIENT = "yarnClient";

	public static final String PREDICATESERIALIZABLE = "PredicateSerializable";
	public static final String MAPFUNCTION = "MapFunction";
	public static final String REDUCEFUNCTION = "ReduceFunction";
	public static final String MAPPAIRFUNCTION = "MapPairFunction";
	public static final String FLATMAPFUNCTION = "FlatMapFunction";
	public static final String JOINTUPLEPREDICATE = "JoinPredicate";
	public static final String LEFTOUTERJOINTUPLEPREDICATE = "LeftOuterJoinPredicate";
	public static final String RIGHTOUTERJOINTUPLEPREDICATE = "RightOuterJoinPredicate";
	public static final String GROUPBYKEYFUNCTION = "GroupByKeyFunction";
	public static final String AGGREGATEREDUCEFUNCTION = "AggregateReduceFunction";
	public static final String AGGREGATEFUNCTION = "AggregateFunction";
	public static final String SAMPLESUPPLIERINTEGER = "SampleSupplierInteger";
	public static final String SAMPLESUPPLIERPARTITION = "SampleSupplierPartition";
	public static final String UNIONFUNCTION = "UnionFunction";
	public static final String INTERSECTIONFUNCTION = "IntersectionFunction";
	public static final String PAIRFLATMAPFUNCTION = "PairFlatMapFunction";
	public static final String LONGFLATMAPFUNCTION = "LongFlatMapFunction";
	public static final String DOUBLEFLATMAPFUNCTION = "DoubleFlatMapFunction";
	public static final String COALESCEFUNCTION = "CoalesceFunction";
	public static final String COALESCE = "Coalesce";
	public static final String COUNTBYKEYFUNCTION = "CountByKeyFunction";
	public static final String COUNTBYVALUEFUNCTION = "CountByValueFunction";
	public static final String FOLDBYKEY = "FoldByKey";
	public static final String JSON = "Json";
	public static final String CSVOPTIONS = "CsvOptions";
	public static final String PEEKCONSUMER = "PeekConsumer";
	public static final String SORTEDCOMPARATOR = "SortedComparator";
	public static final String CALCULATECOUNT = "CalculateCount";
	public static final String SUMMARYSTATISTICS = "SummaryStatistics";
	public static final String SUM = "Sum";
	public static final String MAX = "Max";
	public static final String MIN = "Min";
	public static final String STANDARDDEVIATION = "StandardDeviation";
	public static final String MAPVALUESFUNCTION = "MapValuesFunction";
	public static final String REDUCEFUNCTIONVALUES = "ReduceFunctionValues";
	public static final String MAPTOINT = "MapToInt";
	public static final String PIPELINEINTSTREAMCOLLECT = "PipelineIntStreamCollect";
	public static final String INTUNARYOPERATOR = "IntUnaryOperator";
	public static final String DATAMASTER = "datamaster";
	public static final String DATAMASTER_HOST = "datamaster.host";
	public static final String DATAMASTER_PORT = "datamaster.port";
	public static final String DATAMASTER_WEBPORT = "datamaster.webport";
	public static final String DATAMASTER_MULTICAST_HOST = "datamaster.multicast.host";
	public static final String DATAMASTER_MULTICAST_PORT = "datamaster.multicast.port";
	public static final String DATAMASTER_PINGDELAY = "datamaster.pingdelay";
	public static final String DATAMASTER_METASTORE = "datamaster.metastore";
	public static final String DATAREPLICATORWRITER_PORT = "datareplicatorwriter.port";
	public static final String TASKSCHEDULER_WEB_PORT = "taskscheduler.webport";
	public static final String TASKSCHEDULERSTREAM_WEB_PORT = "taskschedulerstream.webport";
	public static final String FILE = "file";
	public static final String CHUNKMETA = "chunkmeta";
	public static final String CHUNKDATA = "chunkdata";
	public static final String CURRENTFILEEXTN = ".current";
	public static final String APPLICATION_OCTETSTREAM = "APPLICATION/OCTET-STREAM";
	public static final String CONTENT_DISPOSITION = "Content-Disposition";
	public static final String CHUNKNAME = "chunkname";
	public static final String CHUNKID = "chunkid";
	public static final String HOSTPORT = "hostport";
	public static final String DATAREPLICATOR_PORT = "datareplicator.port";
	public static final String DATAREPLICATOR_OFFSET = "datareplicator.offset";
	public static final String DATAREPLICATOR_PINGDELAY = "datareplicator.pingdelay";
	public static final String DATAREPLICATOR_HOST = "datareplicator.host";
	public static final String ISUSERDEFINEDBLOCKSIZE = "block.isuserdefined";

	public static final String DATAREPLICATOR_PATH = "datareplicator.path";
	public static final String TEXTHTML = "text/html";
	public static final String TEXTJAVASCRIPT = "text/javascript";
	public static final String TEXTCSS = "text/css";
	public static final String WEB_FOLDER = "web";
	public static final String RESOURCES = "resources";
	public static final String DATA = "data";

	static final String THISHOST = "0.0.0.0";
	public static final String HTTP = "http://";

	public static final String YARNMDCJOBID = "YARNMDCJOBID";

	public static final String SHDP_CONTAINERID = "SHDP_CONTAINERID";

	public static final String PARTITION = "Partition-";
	public static final String DIVIDER = "----------------------------------------------------------------------------------------------------------------------------------------------------------";

	public static final String HIBCFG = "HIBCFG";

	public static final String STAGEEXECUTORS = "se";
	public static final String MDCJOBID = "mdcjobid";

	// Jgroups Cluster
	public static final String CLUSTERNAME = "jgroups.clustername";
	static final String BINDADDRESS = "jgroups.bind_addr";
	static final String JGROUPSMCASTADDR = "jgroups.udp.mcast_addr";
	static final String JGROUPSMCASTPORT = "jgroups.udp.mcast_port";
	public static final String JGROUPSCONF = "jgroups.conffilepath";

	//Graph File Path
	public static final String GRAPHSTOREENABLE = "graph.filestore.enable";
	public static final String GRAPDIRPATH = "graph.file.dir.path";
	public static final String GRAPHFILESTAGESPLANNAME = "graph.stages.file";
	public static final String GRAPHFILEPEPLANNAME = "graph.peplan.file";
	public static final String GRAPHTASKFILENAME = "graph.task.file";

	//Cache Properties
	public static final String CACHESIZEGB = "cache.size";
	public static final String CACHEEXPIRY = "cache.expiry";
	public static final String CACHEDURATION = "cache.duration";
	public static final String BLOCKCACHE = "BlockCache";
	public static final String CACHEDISKSIZEGB = "cache.disk";
	public static final String CACHEDISKPATH = "cache.disk.path";
	public static final String CACHEDISKPATH_DEFAULT = "../cache";


	public static final int MB = 1048576;


	public static final String G1GC = "-XX:+UseG1GC";
	public static final String ZGC = "-XX:+UseZGC";

	public static final String PROPLOADERCONFIGFOLDER = "1";
	public static final String PROPLOADERCLASSPATH = "2";
	public static final long GB = 1073741824;

	public static final String TEPROPLOADDISTROCONFIG = "1";
	public static final String TEPROPLOADCLASSPATHCONFIG = "2";
	public static final String TEPROPLOADCLASSPATHCONFIGEXCEPTION = "3";

	public static final String USERDIR = "user.dir";

	public static final String DUMMYCONTAINER = "127.0.0.1_10101";
	public static final String DUMMYNODE = "127.0.0.1_12121";

	public static final String EXECUTIONPHASE = "executor.phase";

	public static final String MODE = "executor.mode";
	public static final String MODE_DEFAULT = "IGNITE";
	public static final String MODE_NORMAL = "NORMAL";
	public static final String MDCCACHE = "MDCCache";
	public static final String MDCCACHEMR = "MDCCacheMR";
	public static final String IGNITEHOSTPORT = "ignite.hostport";
	public static final String IGNITEHOSTPORT_DEFAULT = "127.0.0.1:47500..47509";
	public static final String IGNITEBACKUP = "ignite.backup";
	public static final String IGNITEBACKUP_DEFAULT = "1";
	public static final String FILEBLOCKSPARTITIONINGERROR = "Unable to partition file blocks";
	public static final String IGNITEMULTICASTGROUP = "ignite.multicastgroup";
	public static final String IGNITEMULTICASTGROUP_DEFAULT = "228.10.10.157";

	public static final String EXECMODE = "taskscheduler.execmode";
	public static final String EXECMODE_DEFAULT = "standlalone";
	public static final String EXECMODE_YARN = "yarn";
	public static final String EXECMODE_IGNITE = "ignite";

	public static final String MASSIVEDATA_YARNINPUT_MAPPER = "mapper.yarn";
	public static final String MASSIVEDATA_YARNINPUT_COMBINER = "combiner.yarn";
	public static final String MASSIVEDATA_YARNINPUT_REDUCER = "reducer.yarn";
	public static final String MASSIVEDATA_YARNINPUT_FILEBLOCKS = "fileblocks.yarn";
	public static final String MASSIVEDATA_YARNINPUT_CONFIGURATION = "jobconf.yarn";

	public static final String CONTEXT_FILE_CLIENT = "yarn-application-context.xml";
	public static final String MAPPER = "mapper";
	public static final String REDUCER = "reducer";

	public static final String EXECUTIONCOUNT = "execution.count";
	public static final String EXECUTIONCOUNT_DEFAULT = "2";

	public static final String JAR = "jar";
	public static final String ARGS = "args";
	public static final String MRJARREQUIRED = "MR Jar Path Is Required";
	public static final String ARGUEMENTSOPTIONAL = "Arguments are Optional";
	public static final String ANTFORMATTER = "ant";

	public static final String YARNRM = "yarn.rm";
	public static final String YARNRM_DEFAULT = "127.0.0.1:8032";
	public static final String YARNSCHEDULER = "yarn.scheduler";
	public static final String YARNSCHEDULER_DEFAULT = "127.0.0.1:8030";
	public static final String YARNFOLDER = "../yarn";
	public static final String YARNOUTJAR = "mdchadoop.jar";

	public static final String FILENAME = "filename";
	public static final String CSS = "css";
	public static final String JAVASCRIPT = "js";

	public static final String YARN = "YARN";
	public static final String MESOS = "Mesos";
	public static final String JGROUPS = "JGroups";
	public static final String LOCAL = "Local";
	public static final String STANDALONE = "Standalone";

	public static final String TSSHA = "TSSHA";
	public static final String JARLOADED = "Jar Loaded";

	public static final String BYTEBUFFERPOOL_MAX = "bytebufferpool.size";
	public static final String BYTEBUFFERPOOL_MAX_DEFAULT = "3";
	public static final String BYTEBUFFERPOOL_BLOCK = "bytebufferpool.blockwhenexhausted";
	public static final String BYTEBUFFERPOOL_BLOCK_DEFAULT = "true";

	public enum STORAGE {
		INMEMORY,DISK,INMEMORY_DISK
	}
	public static final String STORAGEPROP = "storage.type";
	public static final String STORAGEPROP_DEFAULT = STORAGE.INMEMORY.name();

	public static final String DFSOUTPUTFILEREPLICATION = "dfs.replication";
	public static final String DFSOUTPUTFILEREPLICATION_DEFAULT = "1";
	public static final String HDFSNAMENODEURL = "hdfs.namenode.url";
	public static final String HDFSNAMENODEURL_DEFAULT = "hdfs://localhost:9000";


	public static final String BR = "<BR/>";

	public static final String HEAP_PERCENTAGE = "heap.percent";
	public static final String HEAP_PERCENTAGE_DEFAULT = "40";

	public static final String USEGLOBALTASKEXECUTORS = "taskexecutors.isglobal";
	public static final String USEGLOBALTASKEXECUTORS_DEFAULT = "false";

	public static final String CONTAINERALLOCATIONERROR = "Container Allocation Error";

	public static final String TMPDIR = "java.io.tmpdir";

	public static final String IMPLICIT_CONTAINER_ALLOC_NUMBER = "containers.alloc.implicit.number";
	public static final String IMPLICIT_CONTAINER_ALLOC_NUMBER_DEFAULT = "1";
	public static final String IMPLICIT_CONTAINER_ALLOC_CPU = "containers.alloc.implicit.cpu";
	public static final String IMPLICIT_CONTAINER_ALLOC_CPU_DEFAULT = "1";
	public static final String IMPLICIT_CONTAINER_ALLOC_MEMORY = "containers.alloc.implicit.memory";
	public static final String IMPLICIT_CONTAINER_ALLOC_MEMORY_DEFAULT = "GB";
	public static final String IMPLICIT_CONTAINER_ALLOC_MEMORY_SIZE = "containers.alloc.implicit.memory.size";
	public static final String IMPLICIT_CONTAINER_ALLOC_MEMORY_SIZE_DEFAULT = "1";

	public static final String COLOR_PICKER_PRIMARY = "ui.color.primary";
	public static final String COLOR_PICKER_PRIMARY_DEFAULT = "#6E5CDB";
	public static final String COLOR_PICKER_ALTERNATE = "ui.color.alternate";
	public static final String COLOR_PICKER_ALTERNATE_DEFAULT = "#ddddddd";

}
