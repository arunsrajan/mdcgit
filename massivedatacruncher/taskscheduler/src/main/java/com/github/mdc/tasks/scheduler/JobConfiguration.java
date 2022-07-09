package com.github.mdc.tasks.scheduler;

import java.util.Objects;

import com.esotericsoftware.kryo.io.Output;

public class JobConfiguration {
	private Output output;
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
	byte[] mrjar;
	String minmem;
	String maxmem;
	String gctype;
	String numberofcontainers;
	String isblocksuserdefined;
	String execmode;
	String outputfolder;
	String taskexeccount;
	String ignitemulticastgroup;
	String ignitebackup;
	String yarnrm;
	String yarnscheduler;
	String containeralloc;
	String heappercentage;

	public JobConfiguration(String hdfsurl, String tstempdir, String tshost, String tsport, String zkport,
			String zkretrydelay, String tspingdelay, String tsrescheduledelay, String tsinitialdelay,
			String tepingdelay, Boolean hdfs, String blocksize, String batchsize, String numofreducers, String minmem,
			String maxmem, String gctype, String numberofcontainers, String isblocksuserdefined, String execmode
		, String taskexeccount, String ignitemulticastgroup, String ignitebackup, String yarnrm, String yarnscheduler,
			String containeralloc, String heappercentage) {
		this.hdfsurl = hdfsurl;
		this.tstempdir = tstempdir;
		this.tshost = tshost;
		this.tsport = tsport;
		this.zkport = zkport;
		this.zkretrydelay = zkretrydelay;
		this.tspingdelay = tspingdelay;
		this.tsrescheduledelay = tsrescheduledelay;
		this.tsinitialdelay = tsinitialdelay;
		this.tepingdelay = tepingdelay;
		this.hdfs = hdfs;
		this.blocksize = blocksize;
		this.batchsize = batchsize;
		this.numofreducers = numofreducers;
		this.minmem = minmem;
		this.maxmem = maxmem;
		this.gctype = gctype;
		this.numberofcontainers = numberofcontainers;
		this.isblocksuserdefined = isblocksuserdefined;
		this.execmode = execmode;
		this.taskexeccount = taskexeccount;
		this.ignitebackup = ignitebackup;
		this.ignitemulticastgroup = ignitemulticastgroup;
		this.yarnrm = yarnrm;
		this.yarnscheduler = yarnscheduler;
		this.containeralloc = containeralloc;
		this.heappercentage = heappercentage;
	}

	public String getHdfsurl() {
		return hdfsurl;
	}

	public String getTstempdir() {
		return tstempdir;
	}

	public String getTshost() {
		return tshost;
	}

	public String getTsport() {
		return tsport;
	}

	public String getZkport() {
		return zkport;
	}

	public String getZkretrydelay() {
		return zkretrydelay;
	}

	public String getTspingdelay() {
		return tspingdelay;
	}

	public String getTsrescheduledelay() {
		return tsrescheduledelay;
	}

	public String getTsinitialdelay() {
		return tsinitialdelay;
	}

	public String getTepingdelay() {
		return tepingdelay;
	}

	public Boolean getHdfs() {
		return hdfs;
	}

	public String getBlocksize() {
		return blocksize;
	}

	public String getBatchsize() {
		return batchsize;
	}

	public String getNumofreducers() {
		return numofreducers;
	}

	public void setNumofreducers(String numofreducers) {
		this.numofreducers = numofreducers;
	}

	public byte[] getMrjar() {
		return mrjar;
	}

	public void setMrjar(byte[] mrjar) {
		if (!Objects.isNull(this.mrjar)) {
			throw new UnsupportedOperationException();
		}
		this.mrjar = mrjar;
	}

	public String getMinmem() {
		return minmem;
	}

	public void setMinmem(String minmem) {
		this.minmem = minmem;
	}

	public String getMaxmem() {
		return maxmem;
	}

	public void setMaxmem(String maxmem) {
		this.maxmem = maxmem;
	}

	public String getGctype() {
		return gctype;
	}

	public void setGctype(String gctype) {
		this.gctype = gctype;
	}

	public String getNumberofcontainers() {
		return numberofcontainers;
	}

	public void setNumberofcontainers(String numberofcontainers) {
		this.numberofcontainers = numberofcontainers;
	}

	public void setHdfsurl(String hdfsurl) {
		this.hdfsurl = hdfsurl;
	}

	public void setTstempdir(String tstempdir) {
		this.tstempdir = tstempdir;
	}

	public void setTshost(String tshost) {
		this.tshost = tshost;
	}

	public void setTsport(String tsport) {
		this.tsport = tsport;
	}

	public void setZkport(String zkport) {
		this.zkport = zkport;
	}

	public void setZkretrydelay(String zkretrydelay) {
		this.zkretrydelay = zkretrydelay;
	}

	public void setTspingdelay(String tspingdelay) {
		this.tspingdelay = tspingdelay;
	}

	public void setTsrescheduledelay(String tsrescheduledelay) {
		this.tsrescheduledelay = tsrescheduledelay;
	}

	public void setTsinitialdelay(String tsinitialdelay) {
		this.tsinitialdelay = tsinitialdelay;
	}

	public void setTepingdelay(String tepingdelay) {
		this.tepingdelay = tepingdelay;
	}

	public void setHdfs(Boolean hdfs) {
		this.hdfs = hdfs;
	}

	public void setBlocksize(String blocksize) {
		this.blocksize = blocksize;
	}

	public void setBatchsize(String batchsize) {
		this.batchsize = batchsize;
	}

	public Output getOutput() {
		return output;
	}

	public void setOutput(Output output) {
		this.output = output;
	}

	public String getIsblocksuserdefined() {
		return isblocksuserdefined;
	}

	public void setIsblocksuserdefined(String isblocksuserdefined) {
		this.isblocksuserdefined = isblocksuserdefined;
	}

	public String getExecmode() {
		return execmode;
	}

	public void setExecmode(String execmode) {
		this.execmode = execmode;
	}

	public String getOutputfolder() {
		return outputfolder;
	}

	public void setOutputfolder(String outputfolder) {
		this.outputfolder = outputfolder;
	}

	public String getTaskexeccount() {
		return taskexeccount;
	}

	public void setTaskexeccount(String taskexeccount) {
		this.taskexeccount = taskexeccount;
	}

	public String getIgnitemulticastgroup() {
		return ignitemulticastgroup;
	}

	public void setIgnitemulticastgroup(String ignitemulticastgroup) {
		this.ignitemulticastgroup = ignitemulticastgroup;
	}

	public String getIgnitebackup() {
		return ignitebackup;
	}

	public void setIgnitebackup(String ignitebackup) {
		this.ignitebackup = ignitebackup;
	}

	public String getYarnrm() {
		return yarnrm;
	}

	public void setYarnrm(String yarnrm) {
		this.yarnrm = yarnrm;
	}

	public String getYarnscheduler() {
		return yarnscheduler;
	}

	public void setYarnscheduler(String yarnscheduler) {
		this.yarnscheduler = yarnscheduler;
	}

	public String getContaineralloc() {
		return containeralloc;
	}

	public void setContaineralloc(String containeralloc) {
		this.containeralloc = containeralloc;
	}

	public String getHeappercentage() {
		return heappercentage;
	}

	public void setHeappercentage(String heappercentage) {
		this.heappercentage = heappercentage;
	}

}
