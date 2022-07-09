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
package com.github.mdc.tasks.scheduler;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;

public class JobConfigurationBuilder {
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
	String containeralloc;
	String heappercentage;

	private JobConfigurationBuilder() {
		hdfsurl = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL);
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
		yarnrm = MDCProperties.get().getProperty(MDCConstants.YARNRM, MDCConstants.YARNRM_DEFAULT);
		;
		yarnscheduler = MDCProperties.get().getProperty(MDCConstants.YARNSCHEDULER, MDCConstants.YARNSCHEDULER_DEFAULT);
		;
		;
		containeralloc = MDCProperties.get().getProperty(MDCConstants.CONTAINER_ALLOC, MDCConstants.CONTAINER_ALLOC_DEFAULT);
		heappercentage = MDCProperties.get().getProperty(MDCConstants.HEAP_PERCENTAGE, MDCConstants.HEAP_PERCENTAGE_DEFAULT);
	}

	public static JobConfigurationBuilder newBuilder() {
		return new JobConfigurationBuilder();
	}

	public JobConfigurationBuilder setHdfsurl(String hdfsurl) {
		this.hdfsurl = hdfsurl;
		return this;
	}

	public JobConfigurationBuilder setTstempdir(String tstempdir) {
		this.tstempdir = tstempdir;
		return this;
	}

	public JobConfigurationBuilder setTshost(String tshost) {
		this.tshost = tshost;
		return this;
	}

	public JobConfigurationBuilder setTsport(String tsport) {
		this.tsport = tsport;
		return this;
	}

	public JobConfigurationBuilder setZkport(String zkport) {
		this.zkport = zkport;
		return this;
	}

	public JobConfigurationBuilder setZkretrydelay(String zkretrydelay) {
		this.zkretrydelay = zkretrydelay;
		return this;
	}

	public JobConfigurationBuilder setTspingdelay(String tspingdelay) {
		this.tspingdelay = tspingdelay;
		return this;
	}

	public JobConfigurationBuilder setTsrescheduledelay(String tsrescheduledelay) {
		this.tsrescheduledelay = tsrescheduledelay;
		return this;
	}

	public JobConfigurationBuilder setTsinitialdelay(String tsinitialdelay) {
		this.tsinitialdelay = tsinitialdelay;
		return this;
	}

	public JobConfigurationBuilder setTepingdelay(String tepingdelay) {
		this.tepingdelay = tepingdelay;
		return this;
	}

	public JobConfigurationBuilder setHdfs(Boolean hdfs) {
		this.hdfs = hdfs;
		return this;
	}

	public JobConfigurationBuilder setBlocksize(String blocksize) {
		this.blocksize = blocksize;
		return this;
	}

	public JobConfigurationBuilder setBatchsize(String batchsize) {
		this.batchsize = batchsize;
		return this;
	}

	public JobConfigurationBuilder setNumofreducers(String numofreducers) {
		this.numofreducers = numofreducers;
		return this;
	}

	public JobConfiguration build() {
		return new JobConfiguration(hdfsurl, tstempdir, tshost, tsport, zkport, zkretrydelay, tspingdelay,
				tsrescheduledelay, tsinitialdelay, tepingdelay, hdfs, blocksize, batchsize, numofreducers, minmem,
				maxmem, gctype, numberofcontainers, isblocksuserdefined, execmode, taskexeccount,
				ignitemulticastgroup, ignitebackup, yarnrm, yarnscheduler, containeralloc, heappercentage);

	}

	public String getMinmem() {
		return minmem;
	}

	public JobConfigurationBuilder setMinmem(String minmem) {
		this.minmem = minmem;
		return this;
	}

	public String getMaxmem() {
		return maxmem;
	}

	public JobConfigurationBuilder setMaxmem(String maxmem) {
		this.maxmem = maxmem;
		return this;
	}

	public String getGctype() {
		return gctype;
	}

	public JobConfigurationBuilder setGctype(String gctype) {
		this.gctype = gctype;
		return this;
	}

	public String getNumberofcontainers() {
		return numberofcontainers;
	}


	public JobConfigurationBuilder setIsblocksuserdefined(String isblocksuserdefined) {
		this.isblocksuserdefined = isblocksuserdefined;
		return this;
	}

	public JobConfigurationBuilder setNumberofcontainers(String numberofcontainers) {
		this.numberofcontainers = numberofcontainers;
		return this;
	}

	public String getExecmode() {
		return execmode;
	}

	public JobConfigurationBuilder setExecmode(String execmode) {
		this.execmode = execmode;
		return this;
	}

	public JobConfigurationBuilder setTaskexeccount(String taskexeccount) {
		this.taskexeccount = taskexeccount;
		return this;
	}

	public JobConfigurationBuilder setIgnitemulticastgroup(String ignitemulticastgroup) {
		this.ignitemulticastgroup = ignitemulticastgroup;
		return this;
	}

	public JobConfigurationBuilder setIgnitebackup(String ignitebackup) {
		this.ignitebackup = ignitebackup;
		return this;
	}

	public JobConfigurationBuilder setYarnrm(String yarnrm) {
		this.yarnrm = yarnrm;
		return this;
	}

	public JobConfigurationBuilder setYarnscheduler(String yarnscheduler) {
		this.yarnscheduler = yarnscheduler;
		return this;
	}

	public JobConfigurationBuilder setContaineralloc(String containeralloc) {
		this.containeralloc = containeralloc;
		return this;
	}

	public JobConfigurationBuilder setHeappercentage(String heappercentage) {
		this.heappercentage = heappercentage;
		return this;
	}


}
