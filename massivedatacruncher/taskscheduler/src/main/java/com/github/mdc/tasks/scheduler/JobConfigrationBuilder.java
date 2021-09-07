package com.github.mdc.tasks.scheduler;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;

public class JobConfigrationBuilder {
	String hdfsurl;
	String tstempdir;
	String tshost;
	String tsport;
	String zkport;
	String zkretrydelay;
	String tspingdelay;
	String tsrescheduledelay;
	String tsinitialdelay;
	String tepingdelay;
	Boolean hdfs;
	String blocksize;
	String batchsize;
	String numofreducers;
	String minmem;
	String maxmem;
	String gctype;
	String numberofcontainers;
	String isblocksuserdefined;
	String execmode;
	String taskexeccount;
	String ignitemulticastgroup;
	String ignitebackup;
	String yarnrm;
	String yarnscheduler;
	private JobConfigrationBuilder() {
		hdfsurl = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HDFSNN);
		tstempdir = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_TMP_DIR);
		tshost = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HOST);
		tsport = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT);
		zkport = MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_HOSTPORT);
		zkretrydelay = MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_RETRYDELAY);
		tspingdelay = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PINGDELAY);
		tsrescheduledelay = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_RESCHEDULEDELAY);
		tsinitialdelay = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_INITIALDELAY);
		tepingdelay = MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PINGDELAY);
		hdfs = Boolean.parseBoolean(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_ISHDFS));
		blocksize = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_BLOCKSIZE);
		batchsize = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_BATCHSIZE);
		numofreducers = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_NUMREDUCERS);
		maxmem = MDCProperties.get().getProperty(MDCConstants.MAXMEMORY, MDCConstants.CONTAINER_MAXMEMORY_DEFAULT);
		minmem = MDCProperties.get().getProperty(MDCConstants.MINMEMORY, MDCConstants.CONTAINER_MINMEMORY_DEFAULT);
		gctype = MDCProperties.get().getProperty(MDCConstants.GCCONFIG, MDCConstants.GCCONFIG_DEFAULT);
		numberofcontainers = MDCProperties.get().getProperty(MDCConstants.NUMBEROFCONTAINERS,
				MDCConstants.NUMBEROFCONTAINERS_DEFAULT);
		isblocksuserdefined = MDCProperties.get().getProperty(MDCConstants.ISUSERDEFINEDBLOCKSIZE, MDCConstants.ISUSERDEFINEDBLOCKSIZE_DEFAULT);
		execmode = MDCProperties.get().getProperty(MDCConstants.EXECMODE, MDCConstants.EXECMODE_DEFAULT);
		taskexeccount = MDCProperties.get().getProperty(MDCConstants.EXECUTIONCOUNT, MDCConstants.EXECUTIONCOUNT_DEFAULT);
		ignitemulticastgroup = MDCProperties.get().getProperty(MDCConstants.IGNITEMULTICASTGROUP, MDCConstants.IGNITEMULTICASTGROUP_DEFAULT);
		ignitebackup = MDCProperties.get().getProperty(MDCConstants.IGNITEBACKUP, MDCConstants.IGNITEBACKUP_DEFAULT);
		yarnrm = MDCProperties.get().getProperty(MDCConstants.YARNRM, MDCConstants.YARNRM_DEFAULT);;
		yarnscheduler = MDCProperties.get().getProperty(MDCConstants.YARNSCHEDULER, MDCConstants.YARNSCHEDULER_DEFAULT);;;
	}

	public static JobConfigrationBuilder newBuilder() {
		return new JobConfigrationBuilder();
	}

	public JobConfigrationBuilder setHdfsurl(String hdfsurl) {
		this.hdfsurl = hdfsurl;
		return this;
	}

	public JobConfigrationBuilder setTstempdir(String tstempdir) {
		this.tstempdir = tstempdir;
		return this;
	}

	public JobConfigrationBuilder setTshost(String tshost) {
		this.tshost = tshost;
		return this;
	}

	public JobConfigrationBuilder setTsport(String tsport) {
		this.tsport = tsport;
		return this;
	}

	public JobConfigrationBuilder setZkport(String zkport) {
		this.zkport = zkport;
		return this;
	}

	public JobConfigrationBuilder setZkretrydelay(String zkretrydelay) {
		this.zkretrydelay = zkretrydelay;
		return this;
	}

	public JobConfigrationBuilder setTspingdelay(String tspingdelay) {
		this.tspingdelay = tspingdelay;
		return this;
	}

	public JobConfigrationBuilder setTsrescheduledelay(String tsrescheduledelay) {
		this.tsrescheduledelay = tsrescheduledelay;
		return this;
	}

	public JobConfigrationBuilder setTsinitialdelay(String tsinitialdelay) {
		this.tsinitialdelay = tsinitialdelay;
		return this;
	}

	public JobConfigrationBuilder setTepingdelay(String tepingdelay) {
		this.tepingdelay = tepingdelay;
		return this;
	}

	public JobConfigrationBuilder setHdfs(Boolean hdfs) {
		this.hdfs = hdfs;
		return this;
	}

	public JobConfigrationBuilder setBlocksize(String blocksize) {
		this.blocksize = blocksize;
		return this;
	}

	public JobConfigrationBuilder setBatchsize(String batchsize) {
		this.batchsize = batchsize;
		return this;
	}

	public JobConfigrationBuilder setNumofreducers(String numofreducers) {
		this.numofreducers = numofreducers;
		return this;
	}

	public JobConfiguration build() {
		return new JobConfiguration(hdfsurl, tstempdir, tshost, tsport, zkport, zkretrydelay, tspingdelay,
				tsrescheduledelay, tsinitialdelay, tepingdelay, hdfs, blocksize, batchsize, numofreducers, minmem,
				maxmem, gctype, numberofcontainers, isblocksuserdefined, execmode, taskexeccount, 
				ignitemulticastgroup, ignitebackup, yarnrm, yarnscheduler);
		
	}

	public String getMinmem() {
		return minmem;
	}

	public JobConfigrationBuilder setMinmem(String minmem) {
		this.minmem = minmem;
		return this;
	}

	public String getMaxmem() {
		return maxmem;
	}

	public JobConfigrationBuilder setMaxmem(String maxmem) {
		this.maxmem = maxmem;
		return this;
	}

	public String getGctype() {
		return gctype;
	}

	public JobConfigrationBuilder setGctype(String gctype) {
		this.gctype = gctype;
		return this;
	}

	public String getNumberofcontainers() {
		return numberofcontainers;
	}

	
	
	public JobConfigrationBuilder setIsblocksuserdefined(String isblocksuserdefined) {
		this.isblocksuserdefined = isblocksuserdefined;
		return this;
	}

	public JobConfigrationBuilder setNumberofcontainers(String numberofcontainers) {
		this.numberofcontainers = numberofcontainers;
		return this;
	}

	public String getExecmode() {
		return execmode;
	}

	public JobConfigrationBuilder setExecmode(String execmode) {
		this.execmode = execmode;
		return this;
	}

	public JobConfigrationBuilder setTaskexeccount(String taskexeccount) {
		this.taskexeccount = taskexeccount;
		return this;
	}

	public JobConfigrationBuilder setIgnitemulticastgroup(String ignitemulticastgroup) {
		this.ignitemulticastgroup = ignitemulticastgroup;
		return this;
	}

	public JobConfigrationBuilder setIgnitebackup(String ignitebackup) {
		this.ignitebackup = ignitebackup;
		return this;
	}

	public JobConfigrationBuilder setYarnrm(String yarnrm) {
		this.yarnrm = yarnrm;
		return this;
	}

	public JobConfigrationBuilder setYarnscheduler(String yarnscheduler) {
		this.yarnscheduler = yarnscheduler;
		return this;
	}
	
	
	
}
