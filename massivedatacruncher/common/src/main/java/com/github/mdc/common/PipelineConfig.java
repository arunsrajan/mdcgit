package com.github.mdc.common;

import java.util.Objects;

import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.MDCConstants.STORAGE;

/**
 * 
 * @author arun
 * The configuration for pipeline interfaces.
 */
public class PipelineConfig implements Cloneable{
	private Output output;
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
	
	public void setKryoOutput(Output output) {
		this.output = output;
	}
	public Output getOutput() {
		return output;
	}
	
	public String getBlocksize() {
		return Objects.isNull(blocksize)
				?MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_BLOCKSIZE, MDCConstants.TASKSCHEDULERSTREAM_BLOCKSIZE_DEFAULT):blocksize;
	}
	public void setBlocksize(String blocksize) {
		this.blocksize = blocksize;
	}
	public String getPingdelay() {
		return Objects.isNull(pingdelay)
				?MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PINGDELAY, MDCConstants.TASKSCHEDULERSTREAM_PINGDELAY_DEFAULT):pingdelay;
	}
	public void setPingdelay(String pingdelay) {
		this.pingdelay = pingdelay;
	}
	public String getRescheduledelay() {
		return Objects.isNull(rescheduledelay)
				?MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY, MDCConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY_DEFAULT):rescheduledelay;
	}
	public void setRescheduledelay(String rescheduledelay) {
		this.rescheduledelay = rescheduledelay;
	}
	public String getInitialdelay() {
		return Objects.isNull(initialdelay)
				?MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_INITIALDELAY, MDCConstants.TASKSCHEDULERSTREAM_INITIALDELAY_DEFAULT):initialdelay;
	}
	public void setInitialdelay(String initialdelay) {
		this.initialdelay = initialdelay;
	}
	public String getBatchsize() {
		return Objects.isNull(batchsize)
				?MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_BATCHSIZE, MDCConstants.TASKSCHEDULERSTREAM_BATCHSIZE_DEFAULT):batchsize;
	}
	public void setBatchsize(String batchsize) {
		this.batchsize = batchsize;
	}
	public String getMesos() {
		return Objects.isNull(mesos)
				?MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_ISMESOS, MDCConstants.TASKSCHEDULERSTREAM_ISMESOS_DEFAULT):mesos;
	}
	public void setMesos(String mesos) {
		this.mesos = mesos;
	}
	public String getMesosmaster() {
		return Objects.isNull(mesosmaster)
				?MDCProperties.get().getProperty(MDCConstants.MESOS_MASTER, MDCConstants.MESOS_MASTER_DEFAULT):mesosmaster;
	}
	public void setMesosmaster(String mesosmaster) {
		this.mesosmaster = mesosmaster;
	}
	public String getYarn() {
		return Objects.isNull(yarn)
				?MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_ISYARN, MDCConstants.TASKSCHEDULERSTREAM_ISYARN_DEFAULT):yarn;
	}
	public void setYarn(String yarn) {
		this.yarn = yarn;
	}
	public String getLocal() {
		return Objects.isNull(local)
				?MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_ISLOCAL, MDCConstants.TASKSCHEDULERSTREAM_ISLOCAL_DEFAULT):local;
	}
	public void setLocal(String local) {
		this.local = local;
	}
	public String getJgroups() {
		return Objects.isNull(jgroups)
				?MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_ISJGROUPS, MDCConstants.TASKSCHEDULERSTREAM_ISJGROUPS_DEFAULT):jgroups;
	}
	public void setJgroups(String jgroups) {
		this.jgroups = jgroups;
	}
	public String getRandomte() {
		return Objects.isNull(randomte)
				?MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_RANDTE, MDCConstants.TASKSCHEDULER_RANDTE_DEFAULT):randomte;
	}
	public void setRandomte(String randomte) {
		this.randomte = randomte;
	}
	public void setOutput(Output output) {
		this.output = output;
	}
	public String getMinmem() {
		return Objects.isNull(minmem)
				?MDCProperties.get().getProperty(MDCConstants.MINMEMORY, MDCConstants.CONTAINER_MINMEMORY_DEFAULT):minmem;
	}
	public void setMinmem(String minmem) {
		this.minmem = minmem;
	}
	public String getMaxmem() {
		return Objects.isNull(maxmem)
				?MDCProperties.get().getProperty(MDCConstants.MAXMEMORY, MDCConstants.CONTAINER_MAXMEMORY_DEFAULT): maxmem;
	}
	public void setMaxmem(String maxmem) {
		this.maxmem = maxmem;
	}
	public String getGctype() {
		return Objects.isNull(gctype)
				?MDCProperties.get().getProperty(MDCConstants.GCCONFIG, MDCConstants.GCCONFIG_DEFAULT):gctype;
	}
	public void setGctype(String gctype) {
		this.gctype = gctype;
	}
	public String getNumberofcontainers() {
		return Objects.isNull(numberofcontainers)
				?MDCProperties.get().getProperty(MDCConstants.NUMBEROFCONTAINERS, MDCConstants.NUMBEROFCONTAINERS_DEFAULT):numberofcontainers;
	}
	public void setNumberofcontainers(String numberofcontainers) {
		this.numberofcontainers = numberofcontainers;
	}
	public byte[] getJar() {
		return jar;
	}
	public void setJar(byte[] jar) {
		if(!Objects.isNull(this.jar)) {
			throw new UnsupportedOperationException();
		}
		this.jar = jar;
	}
	public String getIsblocksusedefined() {
		return Objects.isNull(isblocksuserdefined)
				?MDCProperties.get().getProperty(MDCConstants.ISUSERDEFINEDBLOCKSIZE, MDCConstants.ISUSERDEFINEDBLOCKSIZE_DEFAULT):isblocksuserdefined;
	}
	public void setIsblocksuserdefined(String isblocksuserdefined) {
		this.isblocksuserdefined = isblocksuserdefined;
	}
	public String getMode() {
		return Objects.isNull(mode)
				?MDCProperties.get().getProperty(MDCConstants.MODE, MDCConstants.MODE_DEFAULT):mode;
	}
	public void setMode(String mode) {
		this.mode = mode;
	}
	public String getIgnitehp() {
		return Objects.isNull(ignitehp)
				?MDCProperties.get().getProperty(MDCConstants.MODE, MDCConstants.IGNITEHOSTPORT_DEFAULT):ignitehp;
	}
	public void setIgnitehp(String ignitehp) {
		this.ignitehp = ignitehp;
	}
	public String getIgnitebackup() {
		return Objects.isNull(ignitebackup)
				?MDCProperties.get().getProperty(MDCConstants.IGNITEBACKUP, MDCConstants.IGNITEBACKUP_DEFAULT):ignitebackup;
	}
	public void setIgnitebackup(String ignitebackup) {
		this.ignitebackup = ignitebackup;
	}
	public String getIgnitemulticastgroup() {
		return Objects.isNull(ignitemulticastgroup)
				?MDCProperties.get().getProperty(MDCConstants.IGNITEMULTICASTGROUP, MDCConstants.IGNITEMULTICASTGROUP_DEFAULT):ignitemulticastgroup;
	}
	public void setIgnitemulticastgroup(String ignitemulticastgroup) {
		this.ignitemulticastgroup = ignitemulticastgroup;
	}
	public String getExecutioncount() {
		return Objects.isNull(executioncount)
				?MDCProperties.get().getProperty(MDCConstants.EXECUTIONCOUNT, MDCConstants.EXECUTIONCOUNT_DEFAULT):executioncount;
	}
	public void setExecutioncount(String executioncount) {
		this.executioncount = executioncount;
	}
	
	public String getTsshaenabled() {
		return Objects.isNull(tsshaenabled)
				?MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HA_ENABLED, MDCConstants.TASKSCHEDULERSTREAM_HA_ENABLED_DEFAULT):tsshaenabled;
	}
	public void setTsshaenabled(String tsshaenabled) {
		this.tsshaenabled = tsshaenabled;
	}
	
	public STORAGE getStorage() {
		return Objects.isNull(storage)
				?MDCProperties.get().getProperty(MDCConstants.STORAGEPROP, MDCConstants.STORAGEPROP_DEFAULT).equals(MDCConstants.STORAGE.INMEMORY.name())?STORAGE.INMEMORY:STORAGE.DISK:storage;
	}
	public void setStorage(STORAGE storage) {
		this.storage = storage;
	}
	
	public String getContaineralloc() {
		return Objects.isNull(containeralloc)
				?MDCProperties.get().getProperty(MDCConstants.CONTAINER_ALLOC, MDCConstants.CONTAINER_ALLOC_DEFAULT):containeralloc;
	}
	public void setContaineralloc(String containeralloc) {
		this.containeralloc = containeralloc;
	}
	
	public String getHeappercent() {
		return Objects.isNull(heappercent)
				?MDCProperties.get().getProperty(MDCConstants.HEAP_PERCENTAGE, MDCConstants.HEAP_PERCENTAGE_DEFAULT):heappercent;
	}
	
	public Boolean getUseglobaltaskexecutors() {
		return Objects.isNull(useglobaltaskexecutors)
				?Boolean.parseBoolean(MDCProperties.get().getProperty(MDCConstants.USEGLOBALTASKEXECUTORS, MDCConstants.USEGLOBALTASKEXECUTORS_DEFAULT)):useglobaltaskexecutors;
	}
	
	
	
	public String getImplicitcontainerallocanumber() {
		return Objects.isNull(implicitcontainerallocanumber)
				?MDCProperties.get().getProperty(MDCConstants.IMPLICIT_CONTAINER_ALLOC_NUMBER, MDCConstants.IMPLICIT_CONTAINER_ALLOC_NUMBER_DEFAULT):implicitcontainerallocanumber;
	}
	public void setImplicitcontainerallocanumber(String implicitcontainerallocanumber) {
		this.implicitcontainerallocanumber = implicitcontainerallocanumber;
	}
	public String getImplicitcontainercpu() {
		return Objects.isNull(implicitcontainercpu)
				?MDCProperties.get().getProperty(MDCConstants.IMPLICIT_CONTAINER_ALLOC_CPU, MDCConstants.IMPLICIT_CONTAINER_ALLOC_CPU_DEFAULT):implicitcontainercpu;
	}
	public void setImplicitcontainercpu(String implicitcontainercpu) {
		this.implicitcontainercpu = implicitcontainercpu;
	}
	public String getImplicitcontainermemory() {
		return Objects.isNull(implicitcontainermemory)
				?MDCProperties.get().getProperty(MDCConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY, MDCConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_DEFAULT):implicitcontainermemory;
	}
	public void setImplicitcontainermemory(String implicitcontainermemory) {
		this.implicitcontainermemory = implicitcontainermemory;
	}
	public String getImplicitcontainermemorysize() {
		return Objects.isNull(implicitcontainermemorysize)
				?MDCProperties.get().getProperty(MDCConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_SIZE, MDCConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_SIZE_DEFAULT):implicitcontainermemorysize;
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
