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

import java.beans.PropertyChangeListener;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple2;

import com.github.dexecutor.core.DefaultDexecutor;
import com.github.dexecutor.core.DexecutorConfig;
import com.github.dexecutor.core.ExecutionConfig;
import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.ExecutionResults;
import com.github.dexecutor.core.task.Task;
import com.github.dexecutor.core.task.TaskProvider;
import com.github.mdc.common.AllocateContainers;
import com.github.mdc.common.ApplicationTask;
import com.github.mdc.common.ApplicationTask.TaskStatus;
import com.github.mdc.common.ApplicationTask.TaskType;
import com.github.mdc.common.Block;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.ContainerLaunchAttributes;
import com.github.mdc.common.ContainerResources;
import com.github.mdc.common.Context;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.DestroyContainers;
import com.github.mdc.common.Dummy;
import com.github.mdc.common.HDFSBlockUtils;
import com.github.mdc.common.HeartBeat;
import com.github.mdc.common.JobApp;
import com.github.mdc.common.JobMetrics;
import com.github.mdc.common.LaunchContainers;
import com.github.mdc.common.LoadJar;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCJobMetrics;
import com.github.mdc.common.MDCNodesResources;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.PipelineConstants;
import com.github.mdc.common.ReducerValues;
import com.github.mdc.common.Resources;
import com.github.mdc.common.RetrieveData;
import com.github.mdc.common.RetrieveKeys;
import com.github.mdc.common.Tuple3Serializable;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.PipelineException;
import com.github.mdc.stream.scheduler.StreamPipelineTaskSubmitter;
import com.google.common.collect.Iterables;

@SuppressWarnings("rawtypes")
public class MapReduceApplication implements Callable<List<DataCruncherContext>> {
	String jobname;
	JobConfiguration jobconf;
	protected List<MapperInput> mappers;
	protected List<Class<?>> combiners;
	protected List<Class<?>> reducers;
	String outputfolder;
	int batchsize;
	int numreducers;
	public Set<String> hdfsdirpath;
	FileSystem hdfs;
	List<Path> blockpath = new ArrayList<>();
	int totalreadsize;
	byte[] read1byt = new byte[1];
	int blocksize;
	Path currentfilepath;
	int blocklocationindex;
	long redcount;
	List<BlocksLocation> bls;
	List<String> nodes;
	CuratorFramework cf;
	static Logger log = Logger.getLogger(MapReduceApplication.class);
	List<LocatedBlock> locatedBlocks;
	int executorindex;
	ExecutorService es;
	HeartBeat hbs;
	JobMetrics jm = new JobMetrics();
	Map<String, String> apptaskhp = new ConcurrentHashMap<>();
	public MapReduceApplication(String jobname, JobConfiguration jobconf, List<MapperInput> mappers,
			List<Class<?>> combiners, List<Class<?>> reducers, String outputfolder) {
		this.jobname = jobname;
		this.jobconf = jobconf;
		this.mappers = mappers;
		this.combiners = combiners;
		this.reducers = reducers;
		this.outputfolder = outputfolder;
	}


	Map<String, ArrayBlockingQueue> containerqueue = new ConcurrentHashMap<>();
	List<Integer> ports;
	protected List<String> containers;
	protected List<String> nodessorted;
	public List<LaunchContainers> lcs = new ArrayList<>();
	private ConcurrentMap<String, Resources> resources;

	private void getContainersBalanced(List<BlocksLocation> bls) throws MapReduceException {
		log.debug("Entered MdcJob.getContainersBalanced");
		var hostcontainermap = containers.stream()
				.collect(Collectors.groupingBy(key -> key.split(MDCConstants.UNDERSCORE)[0],
						Collectors.mapping(container -> container,
								Collectors.toCollection(ArrayList::new))));
		var containerallocatecount = (Map<String, Long>) containers.stream().parallel().collect(Collectors.toMap(container -> container, container -> 0l));
		List<String> hostportcontainer;
		for (var b : bls) {
			hostportcontainer = hostcontainermap.get(b.getBlock()[0].getHp().split(MDCConstants.COLON)[0]);
			var optional = hostportcontainer.stream().sorted((xref1, xref2) -> {
				return containerallocatecount.get(xref1).compareTo(containerallocatecount.get(xref2));
			}).findFirst();
			if (optional.isPresent()) {
				var container = optional.get();
				b.setExecutorhp(container);
				containerallocatecount.put(container, containerallocatecount.get(container) + 1);
			} else {
				throw new MapReduceException(MDCConstants.CONTAINERALLOCATIONERROR);
			}
		}
		log.debug("Exiting MdcJob.getContainersBalanced");
	}

	Map<String,List<ContainerResources>> nodecrsmap = new ConcurrentHashMap<>();
	
	public void getTaskExecutors(List<BlocksLocation> bls, String appid, String containerid) throws PipelineException {
		try {
			containers = new ArrayList<>();
			var totalcontainersallocated = 0;
			var nodestotalblockmem = new ConcurrentHashMap<String, Long>();
			getNodesResourcesSorted(bls, nodestotalblockmem);
			var resources = MDCNodesResources.get();
			for (var node : nodessorted) {
				var host = node.split("_")[0];
				var lc = new LaunchContainers();
				lc.setNodehostport(node);
				lc.setContainerid(containerid);
				lc.setAppid(appid);
				var cla = new ContainerLaunchAttributes();
				var cr =
						getNumberOfContainers(jobconf.getGctype(), nodestotalblockmem.get(host),
								resources.get(node));
				if (cr.isEmpty()) {
					continue;
				}
				lcs.add(lc);
				cla.setNumberofcontainers(cr.size());
				cla.setCr(cr);
				lc.setCla(cla);
				var ac = new AllocateContainers();
				ac.setContainerid(containerid);
				ac.setNumberofcontainers(cr.size());
				ports = (List<Integer>) Utils.getResultObjectByInput(node, ac);
				Resources allocresources = resources.get(node);
				for (int containercount = 0; containercount < ports.size(); containercount++) {
					ContainerResources crs = cr.get(containercount);
					long maxmemory = crs.getMaxmemory() * MDCConstants.MB;
					long directheap = crs.getDirectheap() *  MDCConstants.MB;
					allocresources.setFreememory(allocresources.getFreememory()-maxmemory-directheap);
					allocresources.setNumberofprocessors(allocresources.getNumberofprocessors()-crs.getCpu());
					crs.setPort(ports.get(containercount));
					containers.add(host + MDCConstants.UNDERSCORE + ports.get(containercount));
				}
				totalcontainersallocated += cr.size();
				nodecrsmap.put(node, cr);
			}
			jm.setContainerresources(lcs.stream().flatMap(lc -> {
				var crs = lc.getCla().getCr();
				return crs.stream().map(cr -> {
					var node = lc.getNodehostport().split(MDCConstants.UNDERSCORE)[0];
					var cpu = cr.getCpu();
					var maxmemory = cr.getMaxmemory();
					var directmemory = cr.getDirectheap();
					var port = cr.getPort();
					return MDCConstants.BR + node + MDCConstants.UNDERSCORE + port + MDCConstants.COLON + MDCConstants.BR + MDCConstants.CPUS
							+ MDCConstants.EQUAL + cpu + MDCConstants.BR + MDCConstants.MEM + MDCConstants.EQUAL
							+ maxmemory + MDCConstants.ROUNDED_BRACKET_OPEN + (Math.floor(maxmemory / (double) (maxmemory + directmemory) * 100.0))
							+ MDCConstants.ROUNDED_BRACKET_CLOSE + MDCConstants.BR + MDCConstants.DIRECTMEM + MDCConstants.EQUAL + directmemory
							+ MDCConstants.ROUNDED_BRACKET_OPEN + (Math.floor(directmemory / (double) (maxmemory + directmemory) * 100.0))
							+ MDCConstants.ROUNDED_BRACKET_CLOSE;

				}).collect(Collectors.toList()).stream();
			}).collect(Collectors.toList()));
			log.debug("Total Containers Allocated:"	+ totalcontainersallocated);
		} catch (Exception ex) {
			log.error(PipelineConstants.TASKEXECUTORSALLOCATIONERROR, ex);
			throw new PipelineException(PipelineConstants.TASKEXECUTORSALLOCATIONERROR, ex);
		}
	}

	public void getDnXref(List<BlocksLocation> bls, boolean issa) throws MapReduceException {
		log.debug("Entered MdcJob.getDnXref");
		var dnxrefs = bls.stream().parallel().flatMap(bl -> {
			var xrefs = new LinkedHashSet<String>();
			Iterator<Set<String>> xref = bl.getBlock()[0].getDnxref().values().iterator();
			for (; xref.hasNext(); ) {
				xrefs.addAll(xref.next());
			}
			if (bl.getBlock().length > 1 && !Objects.isNull(bl.getBlock()[1])) {
				xref = bl.getBlock()[0].getDnxref().values().iterator();
				for (; xref.hasNext(); ) {
					xrefs.addAll(xref.next());
				}
			}
			return xrefs.stream();
		}).collect(Collectors.groupingBy(key -> key.split(MDCConstants.COLON)[0],
				Collectors.mapping(xref -> xref, Collectors.toCollection(LinkedHashSet::new))));
		var dnxrefallocatecount = (Map<String, Long>) dnxrefs.keySet().stream().parallel().flatMap(key -> {
			return dnxrefs.get(key).stream();
		}).collect(Collectors.toMap(xref -> xref, xref -> 0l));
		if (issa) {
			resources = MDCNodesResources.get();
			var computingnodes = resources.keySet().stream().map(node -> node.split(MDCConstants.UNDERSCORE)[0])
					.collect(Collectors.toList());
			for (var b : bls) {
				var xrefselected = b.getBlock()[0].getDnxref().keySet().stream()
						.filter(xrefhost -> computingnodes.contains(xrefhost))
						.flatMap(xrefhost -> b.getBlock()[0].getDnxref().get(xrefhost).stream()).sorted((xref1, xref2) -> {
					return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
				}).findFirst();
				if (xrefselected.isEmpty()) {
					throw new MapReduceException(
							PipelineConstants.INSUFFNODESERROR + " Available computing nodes are "
									+ computingnodes + " Available Data Nodes are " + b.getBlock()[0].getDnxref().keySet());
				}
				final var xref = xrefselected.get();
				dnxrefallocatecount.put(xref, dnxrefallocatecount.get(xref) + 1);
				b.getBlock()[0].setHp(xref);
				if (b.getBlock().length > 1 && !Objects.isNull(b.getBlock()[1])) {
					xrefselected = b.getBlock()[1].getDnxref().keySet().stream()
							.flatMap(xrefhost -> b.getBlock()[1].getDnxref().get(xrefhost).stream())
							.filter(xrefhp -> xrefhp.split(MDCConstants.COLON)[0]
									.equals(xref.split(MDCConstants.COLON)[0]))
							.findFirst();
					if (xrefselected.isEmpty()) {
						xrefselected = b.getBlock()[1].getDnxref().keySet().stream()
								.flatMap(xrefhost -> b.getBlock()[1].getDnxref().get(xrefhost).stream()).findFirst();
						if (xrefselected.isEmpty()) {
							throw new MapReduceException(PipelineConstants.INSUFFNODESERROR
									+ " Available computing nodes are " + computingnodes + " Available Data Nodes are "
									+ b.getBlock()[1].getDnxref().keySet());
						}
					}
					var xref1 = xrefselected.get();
					b.getBlock()[1].setHp(xref1);
				}
			}
		} else {
			for (var b : bls) {
				var xrefselected = b.getBlock()[0].getDnxref().keySet().stream()
						.flatMap(xrefhost -> b.getBlock()[0].getDnxref().get(xrefhost).stream()).sorted((xref1, xref2) -> {
					return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
				}).findFirst();
				var xref = xrefselected.get();
				dnxrefallocatecount.put(xref, dnxrefallocatecount.get(xref) + 1);
				b.getBlock()[0].setHp(xref);
				if (b.getBlock().length > 1 && !Objects.isNull(b.getBlock()[1])) {
					xrefselected = b.getBlock()[1].getDnxref().keySet().stream()
							.flatMap(xrefhost -> b.getBlock()[1].getDnxref().get(xrefhost).stream()).sorted((xref1, xref2) -> {
						return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
					}).findFirst();
					xref = xrefselected.get();
					b.getBlock()[1].setHp(xref);
				}
			}
		}
		log.debug("Exiting MdcJob.getDnXref");
	}

	void getNodesResourcesSorted(List<BlocksLocation> bls, Map<String, Long> nodestotalblockmem) {
		resources = MDCNodesResources.get();

		var nodeswithhostonly = bls.stream().flatMap(bl -> {
			var block1 = bl.getBlock()[0];
			Block block2 = null;
			if (bl.getBlock().length > 1) {
				block2 = bl.getBlock()[1];
			}
			var xref = new HashSet<String>();
			if (!Objects.isNull(block1)) {
				xref.add(block1.getHp().split(MDCConstants.COLON)[0]);
				var value = nodestotalblockmem.get(block1.getHp().split(MDCConstants.COLON)[0]);
				if (value != null) {
					nodestotalblockmem.put(block1.getHp().split(MDCConstants.COLON)[0], value + (block1.getBlockend() - block1.getBlockstart()));
				} else {
					nodestotalblockmem.put(block1.getHp().split(MDCConstants.COLON)[0], block1.getBlockend() - block1.getBlockstart());
				}
			}
			if (!Objects.isNull(block2)) {
				xref.add(block2.getHp().split(MDCConstants.COLON)[0]);
				var value = nodestotalblockmem.get(block2.getHp().split(MDCConstants.COLON)[0]);
				if (value != null) {
					nodestotalblockmem.put(block2.getHp().split(MDCConstants.COLON)[0], value + (block2.getBlockend() - block2.getBlockstart()));
				} else {
					nodestotalblockmem.put(block2.getHp().split(MDCConstants.COLON)[0], block2.getBlockend() - block2.getBlockstart());
				}
			}
			return xref.stream();
		}).collect(Collectors.toSet());
		nodessorted = resources.entrySet().stream().sorted((entry1, entry2) -> {
			var r1 = entry1.getValue();
			var r2 = entry2.getValue();
			if (r1.getNumberofprocessors() < r2.getNumberofprocessors()) {
				return -1;
			} else if (r1.getNumberofprocessors() == r2.getNumberofprocessors()) {
				if (r1.getFreememory() < r2.getFreememory()) {
					return -1;
				} else if (r1.getFreememory() == r2.getFreememory()) {
					return 0;
				}
				else {
					return 1;
				}
			} else {
				return 1;
			}
		}).map(entry -> entry.getKey())
				.filter(key -> nodeswithhostonly.contains(key.split(MDCConstants.UNDERSCORE)[0]))
				.collect(Collectors.toList());
	}

	protected List<ContainerResources> getNumberOfContainers(String gctype, long totalmem, Resources resources)
			throws PipelineException {
		{
			var cpu = resources.getNumberofprocessors() - 1;
			var cr = new ArrayList<ContainerResources>();
			if (jobconf.getContaineralloc().equals(MDCConstants.CONTAINER_ALLOC_DEFAULT)) {
				var res = new ContainerResources();
				var actualmemory = resources.getFreememory() - 256 * MDCConstants.MB;
				if (actualmemory < (128 * MDCConstants.MB)) {
					throw new PipelineException(PipelineConstants.MEMORYALLOCATIONERROR);
				}
				if (totalmem < (512 * MDCConstants.MB) && totalmem > 0 && cpu >= 1) {
					if (actualmemory >= totalmem) {
						res.setCpu(cpu);
						var heapmem = 1024 * Integer.valueOf(jobconf.getHeappercentage()) / 100;
						res.setMinmemory(heapmem);
						res.setMaxmemory(heapmem);
						res.setDirectheap(1024 - heapmem);
						res.setGctype(gctype);
						cr.add(res);
						return cr;
					} else {
						throw new PipelineException(PipelineConstants.INSUFFMEMORYALLOCATIONERROR);
					}
				}
				res.setCpu(cpu);
				var memoryrequire = totalmem < actualmemory ? totalmem : actualmemory;
				var meminmb = memoryrequire / MDCConstants.MB;
				var heapmem = meminmb * Integer.valueOf(jobconf.getHeappercentage()) / 100;
				res.setMinmemory(heapmem);
				res.setMaxmemory(heapmem);
				res.setDirectheap(meminmb - heapmem);
				res.setGctype(gctype);
				cr.add(res);
				return cr;
			} else if (jobconf.getContaineralloc().equals(MDCConstants.CONTAINER_ALLOC_DIVIDED)) {
				var actualmemory = resources.getFreememory() - 256 * MDCConstants.MB;
				if (actualmemory < (128 * MDCConstants.MB)) {
					throw new PipelineException(PipelineConstants.MEMORYALLOCATIONERROR);
				}
				if (totalmem < (512 * MDCConstants.MB) && totalmem > 0 && cpu >= 1) {
					if (actualmemory >= totalmem) {
						var res = new ContainerResources();
						res.setCpu(1);
						var heapmem = 1024 * Integer.valueOf(jobconf.getHeappercentage()) / 100;
						res.setMinmemory(heapmem);
						res.setMaxmemory(heapmem);
						res.setDirectheap(1024 - heapmem);
						res.setGctype(gctype);
						cr.add(res);
						return cr;
					} else {
						throw new PipelineException(PipelineConstants.INSUFFMEMORYALLOCATIONERROR);
					}
				}
				if (cpu == 0) {
					return cr;
				}
				var numofcontainerspermachine = Integer.parseInt(jobconf.getNumberofcontainers());
				var dividedcpus = cpu / numofcontainerspermachine;
				var maxmemory = actualmemory / numofcontainerspermachine;
				var maxmemmb = maxmemory / MDCConstants.MB;
				var totalmemmb = totalmem / MDCConstants.MB;
				if (dividedcpus == 0 && cpu >= 1) {
					dividedcpus = 1;
				}
				if (totalmem < maxmemory && dividedcpus >= 1) {
					var res = new ContainerResources();
					res.setCpu(dividedcpus);
					var heapmem = totalmemmb * Integer.valueOf(jobconf.getHeappercentage()) / 100;
					res.setMinmemory(heapmem);
					res.setMaxmemory(heapmem);
					res.setDirectheap(totalmemmb - heapmem);
					res.setGctype(gctype);
					cr.add(res);
					return cr;
				}
				var numberofcontainer = 0;
				while (true) {
					if (cpu >= dividedcpus && totalmem >= 0) {
						var res = new ContainerResources();
						res.setCpu(dividedcpus);
						var heapmem = maxmemmb * Integer.valueOf(jobconf.getHeappercentage()) / 100;
						res.setMinmemory(heapmem);
						res.setMaxmemory(heapmem);
						res.setDirectheap(maxmemmb - heapmem);
						res.setGctype(gctype);
						cr.add(res);
					} else if (cpu >= 1 && totalmem >= 0) {
						var res = new ContainerResources();
						res.setCpu(cpu);
						var heapmem = maxmemmb * Integer.valueOf(jobconf.getHeappercentage()) / 100;
						res.setMinmemory(heapmem);
						res.setMaxmemory(heapmem);
						res.setDirectheap(maxmemmb - heapmem);
						res.setGctype(gctype);
						cr.add(res);
					} else {
						break;
					}
					numberofcontainer++;
					if (numofcontainerspermachine == numberofcontainer) {
						break;
					}
					cpu -= dividedcpus;
					totalmem -= maxmemory;
				}
				return cr;
			} else if (jobconf.getContaineralloc().equals(MDCConstants.CONTAINER_ALLOC_IMPLICIT)) {
				var actualmemory = resources.getFreememory() - 256 * MDCConstants.MB;
				var numberofimplicitcontainers = Integer.valueOf(jobconf.getImplicitcontainerallocanumber());
				var numberofimplicitcontainercpu = Integer.valueOf(jobconf.getImplicitcontainercpu());
				var numberofimplicitcontainermemory = jobconf.getImplicitcontainermemory();
				var numberofimplicitcontainermemorysize = Long.valueOf(jobconf.getImplicitcontainermemorysize());
				var memorysize = "GB".equals(numberofimplicitcontainermemory) ? MDCConstants.GB : MDCConstants.MB;
				if (actualmemory < numberofimplicitcontainermemorysize * memorysize * numberofimplicitcontainers) {
					throw new PipelineException(PipelineConstants.INSUFFMEMORYALLOCATIONERROR);
				}
				if (cpu < numberofimplicitcontainercpu * numberofimplicitcontainers) {
					throw new PipelineException(PipelineConstants.INSUFFCPUALLOCATIONERROR);
				}
				for (var count = 0; count < numberofimplicitcontainers; count++) {
					var res = new ContainerResources();
					res.setCpu(numberofimplicitcontainercpu);
					var heapmem = numberofimplicitcontainermemorysize * Integer.valueOf(jobconf.getHeappercentage()) / 100;
					res.setMinmemory(heapmem);
					res.setMaxmemory(heapmem);
					res.setDirectheap(numberofimplicitcontainermemorysize - heapmem);
					res.setGctype(gctype);
					cr.add(res);
				}
				return cr;
			} else {
				throw new PipelineException(PipelineConstants.UNSUPPORTEDMEMORYALLOCATIONMODE);
			}
		}
	}
	int containercount;

	public void getContainers(String containerid, String appid) throws Exception {
		var loadjar = new LoadJar();
		loadjar.setMrjar(jobconf.getMrjar());
		List<String> containers = new ArrayList<>();
		for (var lc : lcs) {
			List<Integer> ports = (List<Integer>) Utils.getResultObjectByInput(lc.getNodehostport(), lc);
			int index = 0;
			String tehost = lc.getNodehostport().split(MDCConstants.UNDERSCORE)[0];
			while (index < ports.size()) {
					containers.add(tehost+MDCConstants.UNDERSCORE+ports.get(index));
					while (true) {
						try (Socket sock = new Socket(tehost, ports.get(index))) {
							break;
						} catch (Exception ex) {
							Thread.sleep(1000);
						}
					}
					if (!Objects.isNull(loadjar.getMrjar())) {
						log.info(Utils.getResultObjectByInput(tehost+MDCConstants.UNDERSCORE+ports.get(index), loadjar));
					}
					JobApp jobapp = new JobApp();
					jobapp.setContainerid(lc.getContainerid());
					jobapp.setJobappid(appid);
					jobapp.setJobtype(JobApp.JOBAPP.MR);
					Utils.getResultObjectByInput(tehost + MDCConstants.UNDERSCORE + ports.get(index), jobapp);
					index++;
			}
		}
		this.containers = containers;
	}
	boolean isexception;
	String exceptionmsg = MDCConstants.EMPTY;

	protected List<String> getHostPort(Collection<String> appidtaskids) {
		return appidtaskids.stream().map(apptaskid->apptaskhp.get(apptaskid))
				.collect(Collectors.toList());
	}
	
	@SuppressWarnings({"unchecked"})
	public List<DataCruncherContext> call() {
		var containerid = MDCConstants.CONTAINER + MDCConstants.HYPHEN + System.currentTimeMillis();
		try {
			var starttime = System.currentTimeMillis();
			var containerscount = 0;
			es = Executors.newWorkStealingPool();
			cf = CuratorFrameworkFactory.newClient(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_HOSTPORT),
					20000, 50000, new RetryForever(
							Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_RETRYDELAY))));
			cf.start();

			jm.setJobstarttime(System.currentTimeMillis());
			var isblocksuserdefined = Boolean.parseBoolean(jobconf.getIsblocksuserdefined());
			var applicationid = MDCConstants.MDCAPPLICATION + MDCConstants.HYPHEN + Utils.getUniqueAppID();
			jm.setJobid(applicationid);
			MDCJobMetrics.put(jm);
			batchsize = Integer.parseInt(jobconf.getBatchsize());
			numreducers = Integer.parseInt(jobconf.getNumofreducers());
			var configuration = new Configuration();
			blocksize = Integer.parseInt(jobconf.getBlocksize());
			hdfs = FileSystem.get(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
					configuration);

			var combiner = new HashSet<String>();
			var reducer = new HashSet<>();
			var mapclz = new HashMap<String, Set<String>>();
			hdfsdirpath = new LinkedHashSet<>();
			for (var mapperinput : mappers) {
				try {
					if (mapclz.get(mapperinput.inputfolderpath) == null) {
						mapclz.put(mapperinput.inputfolderpath, new HashSet<>());
					}
					mapclz.get(mapperinput.inputfolderpath).add(mapperinput.crunchmapper.getName());
					hdfsdirpath.add(mapperinput.inputfolderpath);
				} catch (Error ex) {

				}
			}
			Map<String, List<TaskSchedulerMapperCombinerSubmitter>> containermappercombinermap = new ConcurrentHashMap<>();
			var mrtaskcount = 0;
			var folderblocks = new ConcurrentHashMap<String, List<BlocksLocation>>();
			var allfilebls = new ArrayList<BlocksLocation>();
			var allfiles = new ArrayList<String>();
			var fileStatuses = new ArrayList<FileStatus>();
			for (var hdfsdir : hdfsdirpath) {
				var fileStatus = hdfs.listFiles(
						new Path(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL) + hdfsdir), true);
				while (fileStatus.hasNext()) {
					fileStatuses.add(fileStatus.next());
				}
				var paths = FileUtil.stat2Paths(fileStatuses.toArray(new FileStatus[fileStatuses.size()]));
				blockpath.addAll(Arrays.asList(paths));
				bls = new ArrayList<>();
				if (isblocksuserdefined) {
					bls.addAll(HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, blockpath, isblocksuserdefined, blocksize * MDCConstants.MB));
				} else {
					bls.addAll(HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, blockpath, isblocksuserdefined, 128 * MDCConstants.MB));
				}
				folderblocks.put(hdfsdir, bls);
				allfilebls.addAll(bls);
				allfiles.addAll(Utils.getAllFilePaths(blockpath));
				jm.setTotalfilesize(jm.getTotalfilesize() + Utils.getTotalLengthByFiles(hdfs, blockpath));
				blockpath.clear();
				fileStatuses.clear();
			}

			getDnXref(allfilebls, true);
			getTaskExecutors(allfilebls, applicationid, containerid);
			getContainersBalanced(allfilebls);
			getContainers(containerid, applicationid);
			containerscount = containers.size();

			jm.setTotalfilesize(jm.getTotalfilesize() / MDCConstants.MB);
			jm.setFiles(allfiles);
			jm.setNodes(new LinkedHashSet<>(nodessorted));
			jm.setContainersallocated(containers.stream().collect(Collectors.toMap(key -> key, value -> 0d)));
			;
			jm.setMode(jobconf.execmode);
			jm.setTotalblocks(allfilebls.size());
			for (var cls : combiners) {
				if (cls != null) {
					combiner.add(cls.getName());
				}
			}
			for (var cls : reducers) {
				if (reducer != null) {
					reducer.add(cls.getName());
				}
			}			
			for (var folder : hdfsdirpath) {
				var mapclznames = mapclz.get(folder);
				var bls = folderblocks.get(folder);
				for (var bl : bls) {
					var taskid = MDCConstants.TASK + MDCConstants.HYPHEN + (mrtaskcount + 1);
					var apptask = new ApplicationTask();
					apptask.setApplicationid(applicationid);
					apptask.setTaskid(taskid);
					var mdtstm = (TaskSchedulerMapperCombinerSubmitter) getMassiveDataTaskSchedulerThreadMapperCombiner(
							mapclznames, combiner, bl, apptask);
					if (containermappercombinermap.get(mdtstm.getHostPort()) == null) {
						containermappercombinermap.put(mdtstm.getHostPort(), new ArrayList<>());
					}
					containermappercombinermap.get(mdtstm.getHostPort()).add(mdtstm);
					apptaskhp.put(applicationid + taskid, mdtstm.getHostPort());
					mrtaskcount++;
				}

			}
			log.debug("Total MapReduce Tasks: " + mrtaskcount);
			var dccmapphases = new ArrayList<DataCruncherContext>();
			var completed = false;
			var numexecute = 0;
			var taskexeccount = Integer.parseInt(jobconf.getTaskexeccount());
			List<ExecutionResult<TaskSchedulerMapperCombinerSubmitter, Boolean>> erroredresult = null;
			
			while (!completed && numexecute < taskexeccount) {
				DexecutorConfig<DefaultDexecutor, List<ExecutionResult<TaskSchedulerMapperCombinerSubmitter, Boolean>>> config = new DexecutorConfig(newExecutor(), new DTaskExecutor());
				DefaultDexecutor<DefaultDexecutor, List<ExecutionResult<TaskSchedulerMapperCombinerSubmitter, Boolean>>> executor = new DefaultDexecutor<>(config);
				for (var containerkey : containermappercombinermap.keySet()) {
					var dccmapphase = new DataCruncherContext();
					dccmapphases.add(dccmapphase);
					var mdtstms = containermappercombinermap
							.get(containerkey);
					if (!mdtstms.isEmpty()) {
						var clq = new ArrayBlockingQueue(mdtstms.size(), true);
						containerqueue.put(containerkey.trim(), clq);
						DexecutorConfig<TaskSchedulerMapperCombinerSubmitter, Boolean> configexec = new DexecutorConfig(
								newExecutor(),
								new MapCombinerTaskExecutor(new Semaphore(batchsize), clq, dccmapphase, mdtstms.size()));
						DefaultDexecutor<TaskSchedulerMapperCombinerSubmitter, Boolean> dexecutor = new DefaultDexecutor<>(
								configexec);

						for (var mdtstm : mdtstms) {
							dexecutor.addIndependent(mdtstm);
						}
						executor.addIndependent(dexecutor);
					}
				}
				PropertyChangeListener pcl = evt -> {
					try {
						var apptask = (ApplicationTask) evt.getNewValue();
						containerqueue.get(apptask.getHp().trim()).put(apptask);
						log.info(apptask.getHp() + ": " + containerqueue.get(apptask.getHp()));
					} catch (Exception ex) {
						log.info(MDCConstants.EMPTY, ex);
					}
				};
				ExecutionResults<DefaultDexecutor, List<ExecutionResult<TaskSchedulerMapperCombinerSubmitter, Boolean>>> execresults = executor.execute(ExecutionConfig.NON_TERMINATING);
				List<ExecutionResult<DefaultDexecutor, List<ExecutionResult<TaskSchedulerMapperCombinerSubmitter, Boolean>>>> errorresults =  execresults.getAll();
				completed = true;
				var currentcontainers = new ArrayList<String>(containers);
				var containersremoved = new ArrayList<String>(containers);
				containersremoved.removeAll(currentcontainers);
				currentcontainers.stream().forEach(containerhp -> containermappercombinermap.get(containerhp).clear());
				containersremoved.stream().forEach(containerhp -> containermappercombinermap.remove(containerhp));
				for (var execresult :errorresults) {
					erroredresult = execresult.getResult();
					if (!erroredresult.isEmpty()) {
						completed = false;
						erroredresult.stream()
								.forEach(execres -> {
									reConfigureContainerForStageExecution(execres.getId(), currentcontainers);
									containermappercombinermap.get(execres.getId().getHostPort()).add(execres.getId());
								});
					}
				}
				if (!completed) {
					for(ExecutionResult<TaskSchedulerMapperCombinerSubmitter, Boolean> exec:erroredresult)  {
						Utils.writeToOstream(jobconf.getOutput(), MDCConstants.NEWLINE);
						Utils.writeToOstream(jobconf.getOutput(), exec.getId().apptask.getApperrormessage());
					};
				}
				numexecute++;
			}
			if (!Objects.isNull(jobconf.getOutput())) {
				Utils.writeToOstream(jobconf.getOutput(), "Number of Executions: " + numexecute);
			}
			if (!completed) {
				return Arrays.asList(new DataCruncherContext<>());
			}
			var dccred = new ArrayList<DataCruncherContext>();
			List<Tuple3Serializable> keyapptasks;
			var dccmapphase = new DataCruncherContext();
			for (var dcc : dccmapphases) {
				dcc.keys().stream().forEach(dcckey -> {
					dcc.get(dcckey).stream().forEach(dccval -> {
						dccmapphase.put(dcckey, dccval);
					});
				});
			}
			keyapptasks = (List<Tuple3Serializable>) dccmapphase.keys().parallelStream()
					.map(key -> new Tuple3Serializable(key, dccmapphase.get(key), getHostPort(dccmapphase.get(key))))
					.collect(Collectors.toCollection(ArrayList::new));
			var partkeys = Iterables
					.partition(keyapptasks, (keyapptasks.size()) / numreducers).iterator();
			log.info("Keys For Shuffling:" + keyapptasks.size());

			DexecutorConfig<TaskSchedulerReducerSubmitter, Boolean> redconfig = new DexecutorConfig(newExecutor(), new ReducerTaskExecutor(batchsize, applicationid, dccred));
			DefaultDexecutor<TaskSchedulerReducerSubmitter, Boolean> executorred = new DefaultDexecutor<>(redconfig);
			for (; partkeys.hasNext(); ) {
				mrtaskcount++;
				var currentexecutor = getTaskExecutor(mrtaskcount);
				var rv = new ReducerValues();
				rv.setAppid(applicationid);
				rv.setTuples(new ArrayList<>(partkeys.next()));
				rv.setReducerclass(reducers.iterator().next().getName());
				var taskid = MDCConstants.TASK + MDCConstants.HYPHEN + mrtaskcount;
				var mdtstr = new TaskSchedulerReducerSubmitter(
						currentexecutor, rv, applicationid, taskid, redcount, cf, containers);

				log.debug("Reducer: Submitting " + mrtaskcount + " App And Task:"
						+ applicationid + taskid + rv.getTuples());
				if (!Objects.isNull(jobconf.getOutput())) {
					Utils.writeToOstream(jobconf.getOutput(), "Initial Reducer: Submitting " + mrtaskcount + " App And Task:"
							+ applicationid + taskid + rv.getTuples() + " to " + currentexecutor);
				}
				executorred.addIndependent(mdtstr);
			}
			executorred.execute(ExecutionConfig.NON_TERMINATING);
			log.info("Reducer completed------------------------------");
			log.info("Total Tasks Completed: " + mrtaskcount);
			if (!isexception) {
				if (!Objects.isNull(jobconf.getOutput())) {
					Utils.writeToOstream(jobconf.getOutput(), "Reducer completed------------------------------");
				}
				var sb = new StringBuilder();
				var partindex = 1;
				for (var ctxreducerpart :dccred) {
					var keysreducers = ctxreducerpart.keys();
					sb.append(MDCConstants.NEWLINE);
					sb.append("Partition " + partindex + "-------------------------------------------------");
					sb.append(MDCConstants.NEWLINE);
					for (Object key : keysreducers) {
						sb.append(key + MDCConstants.SINGLESPACE + ctxreducerpart.get(key));
						sb.append(MDCConstants.NEWLINE);
					}
					sb.append("-------------------------------------------------");
					sb.append(MDCConstants.NEWLINE);
					sb.append(MDCConstants.NEWLINE);
					partindex++;
				}
				var filename = MDCConstants.MAPRED + MDCConstants.HYPHEN + System.currentTimeMillis();
				log.debug("Writing Results to file: " + filename);
				try (var fsdos = hdfs.create(new Path(
						MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL) + MDCConstants.FORWARD_SLASH
								+ this.outputfolder + MDCConstants.FORWARD_SLASH + filename));) {
					fsdos.write(sb.toString().getBytes());
				} catch (Exception ex) {
					log.error(MDCConstants.EMPTY, ex);
				}
			} else {
				if (!Objects.isNull(jobconf.getOutput())) {
					Utils.writeToOstream(jobconf.getOutput(), exceptionmsg);
				}
			}
			jm.setJobcompletiontime(System.currentTimeMillis());
			jm.setTotaltimetaken((jm.getJobcompletiontime() - jm.getJobstarttime()) / 1000.0);
			if (!Objects.isNull(jobconf.getOutput())) {
				Utils.writeToOstream(jobconf.getOutput(),
						"Completed Job in " + ((System.currentTimeMillis() - starttime) / 1000.0) + " seconds");
			}
			return dccred;
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.info("Unable To Execute Job, See Cause Below:", ex);
		} finally {
			try {
				destroyContainers(containerid);
				if (!Objects.isNull(hbs)) {
					hbs.stop();
					hbs.destroy();
				}
				if (!Objects.isNull(cf)) {
					cf.close();
				}
				if (!Objects.isNull(es)) {
					es.shutdown();
				}
			} catch (Exception ex) {
				log.debug("Resource Release Error", ex);
			}

		}
		return null;
	}

	public void reConfigureContainerForStageExecution(TaskSchedulerMapperCombinerSubmitter mdtsstm,
			List<String> availablecontainers) {
		var bsl = (BlocksLocation) mdtsstm.blockslocation;
		var containersgrouped = availablecontainers.stream()
				.collect(Collectors.groupingBy(key -> key.split(MDCConstants.UNDERSCORE)[0],
						Collectors.mapping(value -> value, Collectors.toCollection(Vector::new))));
		for (var block : bsl.getBlock()) {
			if (!Objects.isNull(block)) {
				var xrefaddrs = block.getDnxref().keySet().stream().map(dnxrefkey -> {
					return block.getDnxref().get(dnxrefkey);
				}).flatMap(xrefaddr -> xrefaddr.stream()).collect(Collectors.toList());

				var containerdnaddr = (List<Tuple2<String, String>>) xrefaddrs.stream()
						.filter(dnxrefkey -> !Objects.isNull(containersgrouped
								.get(dnxrefkey.split(MDCConstants.COLON)[0])))
						.map(dnxrefkey -> {
							List<String> containerstosubmitstage = containersgrouped
									.get(dnxrefkey.split(MDCConstants.COLON)[0]);
							List<Tuple2<String, String>> containerlist = containerstosubmitstage.stream()
									.map(containerhp -> new Tuple2<String, String>(dnxrefkey, containerhp))
									.collect(Collectors.toList());
							return (List<Tuple2<String, String>>) containerlist;
						}).flatMap(containerlist -> containerlist.stream()).collect(Collectors.toList());
				var containerdn = containerdnaddr.get(containercount++ % containerdnaddr.size());
				block.setHp(containerdn.v1);
				bsl.setExecutorhp(containerdn.v2);
				mdtsstm.setHostPort(bsl.getExecutorhp());
			}
		}
	}


	protected class ReducerTaskExecutor implements
			TaskProvider<TaskSchedulerReducerSubmitter, Boolean> {

		Semaphore semaphorereducerresult;
		String applicationid;
		List<DataCruncherContext> dccred;

		protected ReducerTaskExecutor(int batchsize, String applicationid, List<DataCruncherContext> dccred) {
			semaphorereducerresult = new Semaphore(batchsize);
			this.applicationid = applicationid;
			this.dccred = dccred;
		}

		@Override
		public Task<TaskSchedulerReducerSubmitter, Boolean> provideTask(TaskSchedulerReducerSubmitter tsrs) {
			return new Task<TaskSchedulerReducerSubmitter, Boolean>() {
				private static final long serialVersionUID = 8736901461119181694L;

				@Override
				public Boolean execute() {
					try {
						semaphorereducerresult.acquire();
						CountDownLatch cdlreducercomplete = new CountDownLatch(1);
						final PropertyChangeListener reducercompleteobserver = evt -> {
							var apptask = (ApplicationTask) evt.getNewValue();
							if (apptask != null
									&& apptask.getTasktype() == TaskType.REDUCER
									&& apptask.getApplicationid().equals(tsrs.applicationid)
									&& apptask.getTaskid().equals(tsrs.taskid)) {
								try {
									log.debug("Received App And Task Before mutex acquire:" + apptask.getApplicationid()
											+ apptask.getTaskid());
									if (!Objects.isNull(jobconf.getOutput())) {
										Utils.writeToOstream(jobconf.getOutput(),
												"Received App And Task Before mutex acquire:" + apptask.getApplicationid()
														+ apptask.getTaskid());
									}
									if (apptask != null && apptask.getTaskstatus() == TaskStatus.COMPLETED
											&& apptask.getTasktype() == TaskType.REDUCER) {
										semaphorereducerresult.acquire();
										log.debug("Received App And Task After mutex acquire:" + apptask.getApplicationid()
												+ apptask.getTaskid());
										var objects = new ArrayList<>();
										objects.add(new RetrieveData());
										objects.add(apptask.getApplicationid());
										objects.add(apptask.getTaskid());
										log.debug("Received App And Task:" + apptask.getApplicationid() + apptask.getTaskid());
										if (!Objects.isNull(jobconf.getOutput())) {
											Utils.writeToOstream(jobconf.getOutput(),
													"Received App And Task:" + apptask.getApplicationid() + apptask.getTaskid());
										}

										var ctxreducer = (Context) Utils.getResultObjectByInput(apptask.getHp(), objects);
										dccred.add((DataCruncherContext) ctxreducer);
										tsrs.iscompleted = true;
									} else if (apptask != null && apptask.getTaskstatus() == TaskStatus.FAILED
											&& apptask.getTasktype() == TaskType.REDUCER) {
										isexception = true;
										exceptionmsg = apptask.getApperrormessage();
										tsrs.iscompleted = false;
									}
									cdlreducercomplete.countDown();
								} catch (InterruptedException e) {
									log.warn("Interrupted!", e);
									// Restore interrupted state...
									Thread.currentThread().interrupt();
								} catch (Exception ex) {
									log.error(MDCConstants.EMPTY, ex);
								}
							}
						};
						submitReducer(tsrs);

						log.debug("Waiting for the Reducer to complete------------");
						if (!Objects.isNull(jobconf.getOutput())) {
							Utils.writeToOstream(jobconf.getOutput(),
									"Waiting for the Reducer to complete------------");
						}
						cdlreducercomplete.await();
						semaphorereducerresult.release();
					} catch (Exception ex) {
						log.error(MDCConstants.EMPTY, ex);
					}
					return tsrs.iscompleted;
				}
			};
		}

	}

	private class MapCombinerTaskExecutor implements
			TaskProvider<TaskSchedulerMapperCombinerSubmitter, Boolean> {

		Semaphore semaphorebatch;
		ArrayBlockingQueue bq;
		DataCruncherContext dccmapphase;
		CountDownLatch cdl;
		int totaltasks;
		int totalsubmitted;
		ConcurrentMap<String, CountDownLatch> cdls = new ConcurrentHashMap<>();
		ConcurrentMap<String, TaskSchedulerMapperCombinerSubmitter> mdstsmap = new ConcurrentHashMap<>();
		ConcurrentMap<String, Timer> timermap = new ConcurrentHashMap<>();
		private Semaphore totalsubmittedmutex = new Semaphore(1);
		ExecutorService thrpool;

		public MapCombinerTaskExecutor(Semaphore semaphore, ArrayBlockingQueue bq, DataCruncherContext dccmapphase,
				int totaltasks) {
			this.semaphorebatch = semaphore;
			this.bq = bq;
			this.dccmapphase = dccmapphase;
			this.totaltasks = totaltasks;
			thrpool = Executors.newWorkStealingPool();
			cdl = new CountDownLatch(totaltasks);
		}

		public com.github.dexecutor.core.task.Task<TaskSchedulerMapperCombinerSubmitter, Boolean> provideTask(
				final TaskSchedulerMapperCombinerSubmitter mdtstmcmeth) {

			return new com.github.dexecutor.core.task.Task<TaskSchedulerMapperCombinerSubmitter, Boolean>() {
				private TaskSchedulerMapperCombinerSubmitter mdtstmc = mdtstmcmeth;
				private static final long serialVersionUID = 1L;

				public Boolean execute() {
					try {
						semaphorebatch.acquire();
						log.info("Submitting to host " + mdtstmc.getHostPort() + " " + mdtstmc.apptask.getApplicationid()
								+ MDCConstants.HYPHEN + mdtstmc.apptask.getTaskid());
						cdls.put(mdtstmc.apptask.getApplicationid()
								+ mdtstmc.apptask.getTaskid(), new CountDownLatch(1));
						mdstsmap.put(mdtstmc.apptask.getApplicationid()
								+ mdtstmc.apptask.getTaskid(), mdtstmc);
						submitMapper(mdtstmc);
						var timer = new Timer();
						timermap.put(mdtstmc.apptask.getApplicationid()
								+ mdtstmc.apptask.getTaskid(), timer);
						var delay = Long.parseLong(jobconf.getTsinitialdelay());
						timer.scheduleAtFixedRate(new TimerTask() {
							int count = 0;
							@Override
							public void run() {
								try {
									if (++count > 10 ) {
										log.info(mdtstmc.getHostPort() + " Task Failed:" + mdtstmc.apptask.getApplicationid()
												+ mdtstmc.apptask.getTaskid());
										var apptimer = timermap.remove(mdtstmc.apptask.getApplicationid()
												+ mdtstmc.apptask.getTaskid());
										apptimer.cancel();
										apptimer.purge();
									}
								} catch (Exception ex) {
									log.error(MDCConstants.EMPTY, ex);
								}
							}

						}, delay, delay);
						totalsubmittedmutex.acquire();
						totalsubmitted++;
						if (totalsubmitted == 1) {
							thrpool.submit(new ResultProcessor(bq));
						}
						if (totalsubmitted == totaltasks) {
							log.info("All Tasks Submitted: " + totalsubmitted + "/" + totaltasks);
							cdl.await();
							thrpool.shutdown();
						}

						totalsubmittedmutex.release();
						cdls.get(mdtstmc.apptask.getApplicationid()
								+ mdtstmc.apptask.getTaskid()).await();

					} catch (InterruptedException e) {
						log.warn("Interrupted!", e);
						// Restore interrupted state...
						Thread.currentThread().interrupt();
					} catch (Exception ex) {
						log.error("MapCombinerTaskExecutor error", ex);
					}
					if (!mdtstmc.iscompleted) {
						throw new IllegalArgumentException("Incomplete task");
					}
					return mdtstmc.iscompleted;
				}
			};
		}

		class ResultProcessor implements Runnable {
			ArrayBlockingQueue bq;
			int totalouputobtained;
			public ResultProcessor(ArrayBlockingQueue bq) {
				this.bq = bq;
			}

			public void run() {
				while (totalouputobtained < totaltasks) {
					try {
						var apptask = (ApplicationTask) bq.take();
						var mdtstm = mdstsmap.remove(apptask.getApplicationid() + apptask.getTaskid());
						if (!Objects.isNull(mdtstm)) {
							var timer = timermap.remove(apptask.getApplicationid() + apptask.getTaskid());
							if (!Objects.isNull(timer)) {
								timer.cancel();
								timer.purge();
							}
							if (apptask.getTaskstatus() == ApplicationTask.TaskStatus.COMPLETED) {
								log.info("Processing the App Task: " + apptask);
								var objects = new ArrayList<>();
								objects.add(new RetrieveKeys());
								objects.add(apptask.getApplicationid());
								objects.add(apptask.getTaskid());
								var rk = (RetrieveKeys) Utils.getResultObjectByInput(apptask.getHp(), objects);
								dccmapphase.putAll(rk.keys, rk.applicationid + rk.taskid);
								mdtstm.iscompleted = true;
								mdtstm.apptask.setApperrormessage(null);
							} else if (apptask.getTaskstatus() == ApplicationTask.TaskStatus.FAILED) {
								mdtstm.apptask.setApperrormessage(apptask.getApperrormessage());
								mdtstm.iscompleted = false;
							}
							totalouputobtained++;
							double percentcompleted = Math.floor(totalouputobtained / (double) totaltasks * 100.0);
							jm.getContainersallocated().put(apptask.getHp(), percentcompleted);
							if (jobconf.getOutput() != null) {
								Utils.writeToOstream(jobconf.getOutput(), apptask.getTaskid() + " " + apptask.getHp() + "'s Mapper Task Status: " + apptask.getTaskstatus() + " Execution Status = " + totalouputobtained + "/" + totaltasks + " = " + percentcompleted + "%");
							}
							cdls.get(apptask.getApplicationid() + apptask.getTaskid()).countDown();
							semaphorebatch.release();
							cdl.countDown();
						}
					} catch (InterruptedException e) {
						log.warn("Interrupted!", e);
						// Restore interrupted state...
						Thread.currentThread().interrupt();
					} catch (Exception ex) {
						log.info("Mapper Submitted Failed For Getting Response: ", ex);
					}
				}
			}
		}

	}

	private ExecutorService newExecutor() {
		return Executors.newWorkStealingPool();
	}

	private class DTaskExecutor implements
			TaskProvider<DefaultDexecutor<StreamPipelineTaskSubmitter, Boolean>, List<ExecutionResult<StreamPipelineTaskSubmitter, Boolean>>> {

		@Override
		public com.github.dexecutor.core.task.Task<DefaultDexecutor<StreamPipelineTaskSubmitter, Boolean>, List<ExecutionResult<StreamPipelineTaskSubmitter, Boolean>>> provideTask(
				DefaultDexecutor<StreamPipelineTaskSubmitter, Boolean> dexecutor) {
			return new com.github.dexecutor.core.task.Task<DefaultDexecutor<StreamPipelineTaskSubmitter, Boolean>, List<ExecutionResult<StreamPipelineTaskSubmitter, Boolean>>>() {

				private static final long serialVersionUID = 1L;

				@Override
				public List<ExecutionResult<StreamPipelineTaskSubmitter, Boolean>> execute() {
					var execresults = dexecutor.execute(ExecutionConfig.NON_TERMINATING);
					return execresults.getErrored();
				}
			};
		}

	}

	protected void destroyContainers(String containerid) throws Exception {
		var nodes = nodessorted;
		log.debug("Destroying Containers with id:" + containerid + " for the hosts: " + nodes);
		var dc = new DestroyContainers();
		for (var node : nodes) {
			dc.setContainerid(containerid);
			Utils.getResultObjectByInput(node, dc);
			Resources allocresources = MDCNodesResources.get().get(node);
			var crs = nodecrsmap.get(node);
			for(ContainerResources cr: crs) {
				long maxmemory = cr.getMaxmemory() * MDCConstants.MB;
				long directheap = cr.getDirectheap() *  MDCConstants.MB;
				allocresources.setFreememory(allocresources.getFreememory()+maxmemory+directheap);
				allocresources.setNumberofprocessors(allocresources.getNumberofprocessors()+cr.getCpu());
			}
		}
		
	}

	public TaskSchedulerMapperCombinerSubmitter getMassiveDataTaskSchedulerThreadMapperCombiner(
			Set<String> mapclz,Set<String> combiners, BlocksLocation blockslocation, ApplicationTask apptask) throws Exception {
		log.debug("Block To Read :" + blockslocation);
		var mdtstmc = new TaskSchedulerMapperCombinerSubmitter(
				blockslocation, true, new LinkedHashSet<>(mapclz), combiners,
				cf, containers, apptask);
		apptask.setHp(blockslocation.getExecutorhp());
		blocklocationindex++;
		return mdtstmc;
	}

	public String getTaskExecutor(long blocklocationindex) {
		return containers.get((int) blocklocationindex % containers.size());
	}

	public synchronized void submitMapper(TaskSchedulerMapperCombinerSubmitter mdtstmc) throws Exception {
		log.info("Submitting Mapper Task :");
		es.submit(mdtstmc);
	}

	public synchronized void submitReducer(TaskSchedulerReducerSubmitter mdtstr) throws Exception {
		log.info("Submitting Reducer Task :");
		es.submit(mdtstr);
	}
}
