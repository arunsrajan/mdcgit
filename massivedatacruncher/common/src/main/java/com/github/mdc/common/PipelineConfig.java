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

import java.io.OutputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

import org.nustaq.serialization.FSTObjectOutput;

import com.github.mdc.common.MDCConstants.STORAGE;

/**
 * 
 * @author arun
 * The configuration for pipeline interfaces.
 */
public class PipelineConfig implements Serializable, Cloneable {
	private static final long serialVersionUID = 1L;
	private OutputStream  output;
	private String blocksize;
	private String isblocksuserdefined;
	private String pingdelay;
	private String rescheduledelay;
	private String initialdelay;
	private String batchsize;
	private String mesos;
	private String mesosmaster;
	private String yarn;
	private String local;
	private String jgroups;
	private String randomte;
	private String minmem;
	private String maxmem;
	private String gctype;
	private String numberofcontainers;
	private byte[] jar;
	private String mode;
	private String ignitehp;
	private String ignitebackup;
	private String ignitemulticastgroup;
	private String executioncount;
	private String tsshaenabled;
	private STORAGE storage;
	private String containeralloc;
	private String jobname;
	private String heappercent;
	private Boolean useglobaltaskexecutors;
	private String implicitcontainerallocanumber;
	private String implicitcontainercpu;
	private String implicitcontainermemory;
	private String implicitcontainermemorysize;
	private Set<Class<?>> customclasses;

	public void setOutput(OutputStream  output) {
		this.output = output;
	}

	public OutputStream getOutput() {
		return output;
	}

	public String getBlocksize() {
		return Objects.isNull(blocksize)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_BLOCKSIZE, MDCConstants.TASKSCHEDULERSTREAM_BLOCKSIZE_DEFAULT) : MDCConstants.TASKSCHEDULERSTREAM_BLOCKSIZE_DEFAULT : blocksize;
	}

	public void setBlocksize(String blocksize) {
		this.blocksize = blocksize;
	}

	public String getPingdelay() {
		return Objects.isNull(pingdelay)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PINGDELAY, MDCConstants.TASKSCHEDULERSTREAM_PINGDELAY_DEFAULT) : MDCConstants.TASKSCHEDULERSTREAM_PINGDELAY_DEFAULT : pingdelay;
	}

	public void setPingdelay(String pingdelay) {
		this.pingdelay = pingdelay;
	}

	public String getRescheduledelay() {
		return Objects.isNull(rescheduledelay)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY, MDCConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY_DEFAULT) : MDCConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY_DEFAULT : rescheduledelay;
	}

	public void setRescheduledelay(String rescheduledelay) {
		this.rescheduledelay = rescheduledelay;
	}

	public String getInitialdelay() {
		return Objects.isNull(initialdelay)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_INITIALDELAY, MDCConstants.TASKSCHEDULERSTREAM_INITIALDELAY_DEFAULT) : MDCConstants.TASKSCHEDULERSTREAM_INITIALDELAY_DEFAULT : initialdelay;
	}

	public void setInitialdelay(String initialdelay) {
		this.initialdelay = initialdelay;
	}

	public String getBatchsize() {
		return Objects.isNull(batchsize)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_BATCHSIZE, MDCConstants.TASKSCHEDULERSTREAM_BATCHSIZE_DEFAULT) : MDCConstants.TASKSCHEDULERSTREAM_BATCHSIZE_DEFAULT : batchsize;
	}

	public void setBatchsize(String batchsize) {
		this.batchsize = batchsize;
	}

	public String getMesos() {
		return Objects.isNull(mesos)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_ISMESOS, MDCConstants.TASKSCHEDULERSTREAM_ISMESOS_DEFAULT) : MDCConstants.TASKSCHEDULERSTREAM_ISMESOS_DEFAULT : mesos;
	}

	public void setMesos(String mesos) {
		this.mesos = mesos;
	}

	public String getMesosmaster() {
		return Objects.isNull(mesosmaster)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.MESOS_MASTER, MDCConstants.MESOS_MASTER_DEFAULT) : MDCConstants.MESOS_MASTER_DEFAULT : mesosmaster;
	}

	public void setMesosmaster(String mesosmaster) {
		this.mesosmaster = mesosmaster;
	}

	public String getYarn() {
		return Objects.isNull(yarn)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_ISYARN, MDCConstants.TASKSCHEDULERSTREAM_ISYARN_DEFAULT) : MDCConstants.TASKSCHEDULERSTREAM_ISYARN_DEFAULT : yarn;
	}

	public void setYarn(String yarn) {
		this.yarn = yarn;
	}

	public String getLocal() {
		return Objects.isNull(local)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_ISLOCAL, MDCConstants.TASKSCHEDULERSTREAM_ISLOCAL_DEFAULT) : MDCConstants.TASKSCHEDULERSTREAM_ISLOCAL_DEFAULT : local;
	}

	public void setLocal(String local) {
		this.local = local;
	}

	public String getJgroups() {
		return Objects.isNull(jgroups)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_ISJGROUPS, MDCConstants.TASKSCHEDULERSTREAM_ISJGROUPS_DEFAULT) : MDCConstants.TASKSCHEDULERSTREAM_ISJGROUPS_DEFAULT : jgroups;
	}

	public void setJgroups(String jgroups) {
		this.jgroups = jgroups;
	}

	public String getRandomte() {
		return Objects.isNull(randomte)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_RANDTE, MDCConstants.TASKSCHEDULER_RANDTE_DEFAULT) : MDCConstants.TASKSCHEDULER_RANDTE_DEFAULT : randomte;
	}

	public void setRandomte(String randomte) {
		this.randomte = randomte;
	}

	public String getMinmem() {
		return Objects.isNull(minmem)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.MINMEMORY, MDCConstants.CONTAINER_MINMEMORY_DEFAULT) : MDCConstants.CONTAINER_MINMEMORY_DEFAULT : minmem;
	}

	public void setMinmem(String minmem) {
		this.minmem = minmem;
	}

	public String getMaxmem() {
		return Objects.isNull(maxmem)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.MAXMEMORY, MDCConstants.CONTAINER_MAXMEMORY_DEFAULT) : MDCConstants.CONTAINER_MAXMEMORY_DEFAULT : maxmem;
	}

	public void setMaxmem(String maxmem) {
		this.maxmem = maxmem;
	}

	public String getGctype() {
		return Objects.isNull(gctype)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.GCCONFIG, MDCConstants.GCCONFIG_DEFAULT) : MDCConstants.GCCONFIG_DEFAULT : gctype;
	}

	public void setGctype(String gctype) {
		this.gctype = gctype;
	}

	public String getNumberofcontainers() {
		return Objects.isNull(numberofcontainers)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.NUMBEROFCONTAINERS, MDCConstants.NUMBEROFCONTAINERS_DEFAULT) : MDCConstants.NUMBEROFCONTAINERS_DEFAULT : numberofcontainers;
	}

	public void setNumberofcontainers(String numberofcontainers) {
		this.numberofcontainers = numberofcontainers;
	}

	public byte[] getJar() {
		return jar;
	}

	public void setJar(byte[] jar) {
		if (!Objects.isNull(this.jar)) {
			throw new UnsupportedOperationException();
		}
		this.jar = jar;
	}
	
	public void setCustomclasses(Set<Class<?>> customclasses) {
		if (Objects.isNull(customclasses)) {
			throw new UnsupportedOperationException();
		}
		this.customclasses = customclasses;
	}

	public Set<Class<?>> getCustomclasses() {
		return customclasses;
	}

	public String getIsblocksusedefined() {
		return Objects.isNull(isblocksuserdefined)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.ISUSERDEFINEDBLOCKSIZE, MDCConstants.ISUSERDEFINEDBLOCKSIZE_DEFAULT) : MDCConstants.ISUSERDEFINEDBLOCKSIZE_DEFAULT : isblocksuserdefined;
	}

	public void setIsblocksuserdefined(String isblocksuserdefined) {
		this.isblocksuserdefined = isblocksuserdefined;
	}

	public String getMode() {
		return Objects.isNull(mode)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.MODE, MDCConstants.MODE_DEFAULT) : MDCConstants.MODE_DEFAULT : mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public String getIgnitehp() {
		return Objects.isNull(ignitehp)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.MODE, MDCConstants.IGNITEHOSTPORT_DEFAULT) : MDCConstants.IGNITEHOSTPORT_DEFAULT : ignitehp;
	}

	public void setIgnitehp(String ignitehp) {
		this.ignitehp = ignitehp;
	}

	public String getIgnitebackup() {
		return Objects.isNull(ignitebackup)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.IGNITEBACKUP, MDCConstants.IGNITEBACKUP_DEFAULT) : MDCConstants.IGNITEBACKUP_DEFAULT : ignitebackup;
	}

	public void setIgnitebackup(String ignitebackup) {
		this.ignitebackup = ignitebackup;
	}

	public String getIgnitemulticastgroup() {
		return Objects.isNull(ignitemulticastgroup)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.IGNITEMULTICASTGROUP, MDCConstants.IGNITEMULTICASTGROUP_DEFAULT) : MDCConstants.IGNITEMULTICASTGROUP_DEFAULT : ignitemulticastgroup;
	}

	public void setIgnitemulticastgroup(String ignitemulticastgroup) {
		this.ignitemulticastgroup = ignitemulticastgroup;
	}

	public String getExecutioncount() {
		return Objects.isNull(executioncount)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.EXECUTIONCOUNT, MDCConstants.EXECUTIONCOUNT_DEFAULT) : MDCConstants.EXECUTIONCOUNT_DEFAULT : executioncount;
	}

	public void setExecutioncount(String executioncount) {
		this.executioncount = executioncount;
	}

	public String getTsshaenabled() {
		return Objects.isNull(tsshaenabled)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HA_ENABLED, MDCConstants.TASKSCHEDULERSTREAM_HA_ENABLED_DEFAULT) : MDCConstants.TASKSCHEDULERSTREAM_HA_ENABLED_DEFAULT : tsshaenabled;
	}

	public void setTsshaenabled(String tsshaenabled) {
		this.tsshaenabled = tsshaenabled;
	}

	public STORAGE getStorage() {
		return Objects.isNull(storage)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.STORAGEPROP, MDCConstants.STORAGEPROP_DEFAULT).equals(MDCConstants.STORAGE.INMEMORY.name()) ? STORAGE.INMEMORY : STORAGE.DISK : STORAGE.INMEMORY : storage;
	}

	public void setStorage(STORAGE storage) {
		this.storage = storage;
	}

	public String getContaineralloc() {
		return Objects.isNull(containeralloc)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.CONTAINER_ALLOC, MDCConstants.CONTAINER_ALLOC_DEFAULT) : MDCConstants.CONTAINER_ALLOC_DEFAULT : containeralloc;
	}

	public void setContaineralloc(String containeralloc) {
		this.containeralloc = containeralloc;
	}

	public String getHeappercent() {
		return Objects.isNull(heappercent)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.HEAP_PERCENTAGE, MDCConstants.HEAP_PERCENTAGE_DEFAULT) : MDCConstants.HEAP_PERCENTAGE_DEFAULT : heappercent;
	}

	public Boolean getUseglobaltaskexecutors() {
		return Objects.isNull(useglobaltaskexecutors)
				? Boolean.parseBoolean(Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.USEGLOBALTASKEXECUTORS, MDCConstants.USEGLOBALTASKEXECUTORS_DEFAULT) : MDCConstants.USEGLOBALTASKEXECUTORS_DEFAULT) : useglobaltaskexecutors;
	}


	public String getImplicitcontainerallocanumber() {
		return Objects.isNull(implicitcontainerallocanumber)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.IMPLICIT_CONTAINER_ALLOC_NUMBER, MDCConstants.IMPLICIT_CONTAINER_ALLOC_NUMBER_DEFAULT) : MDCConstants.IMPLICIT_CONTAINER_ALLOC_NUMBER_DEFAULT : implicitcontainerallocanumber;
	}

	public void setImplicitcontainerallocanumber(String implicitcontainerallocanumber) {
		this.implicitcontainerallocanumber = implicitcontainerallocanumber;
	}

	public String getImplicitcontainercpu() {
		return Objects.isNull(implicitcontainercpu)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.IMPLICIT_CONTAINER_ALLOC_CPU, MDCConstants.IMPLICIT_CONTAINER_ALLOC_CPU_DEFAULT) : MDCConstants.IMPLICIT_CONTAINER_ALLOC_CPU_DEFAULT : implicitcontainercpu;
	}

	public void setImplicitcontainercpu(String implicitcontainercpu) {
		this.implicitcontainercpu = implicitcontainercpu;
	}

	public String getImplicitcontainermemory() {
		return Objects.isNull(implicitcontainermemory)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY, MDCConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_DEFAULT) : MDCConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_DEFAULT : implicitcontainermemory;
	}

	public void setImplicitcontainermemory(String implicitcontainermemory) {
		this.implicitcontainermemory = implicitcontainermemory;
	}

	public String getImplicitcontainermemorysize() {
		return Objects.isNull(implicitcontainermemorysize)
				? Objects.nonNull(MDCProperties.get()) ? MDCProperties.get().getProperty(MDCConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_SIZE, MDCConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_SIZE_DEFAULT) : MDCConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_SIZE_DEFAULT : implicitcontainermemorysize;
	}

	public void setImplicitcontainermemorysize(String implicitcontainermemorysize) {
		this.implicitcontainermemorysize = implicitcontainermemorysize;
	}

	public void setUseglobaltaskexecutors(Boolean useglobaltaskexecutors) {
		this.useglobaltaskexecutors = useglobaltaskexecutors;
	}

	public void setHeappercent(String heappercent) {
		this.heappercent = heappercent;
	}

	public String getJobname() {
		return jobname;
	}

	public void setJobname(String jobname) {
		this.jobname = jobname;
	}

	@Override
	public PipelineConfig clone() throws CloneNotSupportedException {
		return (PipelineConfig) super.clone();

	}

}
