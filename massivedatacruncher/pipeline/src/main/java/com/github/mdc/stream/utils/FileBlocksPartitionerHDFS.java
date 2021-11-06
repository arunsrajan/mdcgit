package com.github.mdc.stream.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.log4j.Logger;
import org.xerial.snappy.SnappyOutputStream;

import com.github.mdc.common.AllocateContainers;
import com.github.mdc.common.Block;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.CacheAvailability;
import com.github.mdc.common.ContainerLaunchAttributes;
import com.github.mdc.common.ContainerResources;
import com.github.mdc.common.DestroyContainer;
import com.github.mdc.common.DestroyContainers;
import com.github.mdc.common.FileSystemSupport;
import com.github.mdc.common.GlobalContainerAllocDealloc;
import com.github.mdc.common.GlobalContainerLaunchers;
import com.github.mdc.common.HDFSBlockUtils;
import com.github.mdc.common.HdfsBlockReader;
import com.github.mdc.common.Job;
import com.github.mdc.common.LaunchContainers;
import com.github.mdc.common.LoadJar;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCNodesResources;
import com.github.mdc.common.PipelineConstants;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Resources;
import com.github.mdc.common.Stage;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.AbstractPipeline;
import com.github.mdc.stream.IgnitePipeline;
import com.github.mdc.stream.PipelineException;
import com.github.mdc.stream.StreamPipeline;

public class FileBlocksPartitionerHDFS {
	private static Logger log = Logger.getLogger(FileBlocksPartitionerHDFS.class);
	protected long totallength;
	protected List<Path> filepaths = new ArrayList<>();
	protected FileSystem hdfs;
	protected List<String> containers;
	protected Set<String> nodeschoosen;
	protected IntSupplier supplier;
	protected Job job;
	protected PipelineConfig pipelineconfig;
	CacheAvailability cacheavailableresponse = null;
	protected List<String> nodessorted;
	ConcurrentMap<String, Resources> resources;
	CountDownLatch cdl;
	List<String> containerswithhostport;

	Boolean ismesos, isyarn, islocal, isjgroups, isblocksuserdefined, isignite;
	List<Integer> ports;
	
	public List<ContainerResources> getTotalMemoryContainersReuseAllocation(String nodehp, int containerstoallocate) {
		var containers = GlobalContainerAllocDealloc.getNodecontainers().get(nodehp);
		var cres = new ArrayList<ContainerResources>();
		if(!Objects.isNull(containers)&&!containers.isEmpty()) {
			int contcount = 0;
			for(String container:containers) {
				cres.add(GlobalContainerAllocDealloc.getHportcrs().get(container));
				contcount++;
				if(contcount>=containerstoallocate) {
					break;
				}
			}
		}
		return cres;
	}
	
	/**
	 * The block size is determined by sum of length of all files divided by number
	 * of partition.
	 * 
	 * @return
	 * @throws Exception
	 */
	protected List<BlocksLocation> getHDFSParitions() throws PipelineException {
		var numpartition = this.supplier.getAsInt();
		totallength = 0;
		try {
			totallength = Utils.getTotalLengthByFiles(hdfs, filepaths);
			var blocksize = (totallength / numpartition);

			return getBlocks(true,blocksize);
		} catch (Exception ex) {
			log.error(PipelineConstants.FILEIOERROR, ex);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ex);
		}
	}

	private ConcurrentMap<Stage, Object> stageoutputmap = new ConcurrentHashMap<>();
	private ConcurrentMap<String, String> allstageshostport = new ConcurrentHashMap<>();

	/**
	 * Get File Blocks for job
	 * 
	 * @param job
	 * @throws PipelineException
	 * @throws URISyntaxException
	 * @throws IOException
	 * @throws Exception
	 */
	@SuppressWarnings({ "rawtypes" })
	public void getJobStageBlocks(Job job, IntSupplier supplier, String protocol, Set<Stage> rootstages,
			Collection<AbstractPipeline> mdsroots, int blocksize, PipelineConfig pc)
			throws PipelineException, IOException, URISyntaxException {
		try {
			log.debug("Partitioning of Blocks started...");
			this.job = job;
			this.pipelineconfig = pc;
			var roots = mdsroots.iterator();
			var noofpartition = 0l;
			ismesos = Boolean.parseBoolean(pc.getMesos());
			isyarn = Boolean.parseBoolean(pc.getYarn());
			islocal = Boolean.parseBoolean(pc.getLocal());
			isjgroups = Boolean.parseBoolean(pc.getJgroups());
			isblocksuserdefined = Boolean.parseBoolean(pc.getIsblocksusedefined()) || supplier != null;
			isignite = Objects.isNull(pc.getMode())?false:pc.getMode().equals(MDCConstants.MODE_DEFAULT)?true:false;
			if (Boolean.TRUE.equals(islocal) || Boolean.TRUE.equals(ismesos) || Boolean.TRUE.equals(isyarn)
					|| Boolean.TRUE.equals(isignite)) {
				nodeschoosen = new HashSet<>(Arrays.asList(MDCConstants.DUMMYNODE));
				containers = Arrays.asList(MDCConstants.DUMMYCONTAINER);
			}
			
			var totalblockslocation = new ArrayList<BlocksLocation>();
			String hdfspath = null, folder = null;
			List<Path> metricsfilepath = new ArrayList<>();
			job.jm.totalfilesize = 0;
			for (var rootstage : rootstages) {
				var obj = roots.next();
				if(obj instanceof StreamPipeline mdp) {
					hdfspath = mdp.getHdfspath();
					folder = mdp.getFolder();
				} else if(obj instanceof IgnitePipeline mdp) {
					hdfspath = mdp.getHdfspath();
					folder = mdp.getFolder();
				}
				this.filepaths.clear();
				try (var hdfs = FileSystem.newInstance(new URI(hdfspath), new Configuration());) {
					this.hdfs = hdfs;
					this.filepaths.addAll(getFilePaths(hdfspath, folder));
					metricsfilepath.addAll(filepaths);
					if (!stageoutputmap.containsKey(rootstage)) {
						List blocks = null;
						if (supplier instanceof IntSupplier) {
							this.supplier = supplier;
							blocks = getHDFSParitions();
							totalblockslocation.addAll(blocks);
						} else {
							// Get block if HDFS protocol.
							if (protocol.equals(FileSystemSupport.HDFS)) {
								blocks = getBlocks(isblocksuserdefined, blocksize);
								totalblockslocation.addAll(blocks);
							}
						}
						if (blocks == null) {
							throw new PipelineException(MDCConstants.DATABLOCK_EXCEPTION);
						}
						stageoutputmap.put(rootstage, blocks);
						noofpartition += blocks.size();
					}
					job.jm.totalfilesize += Utils.getTotalLengthByFiles(hdfs, this.filepaths);
				}
			}
			job.jm.files = Utils.getAllFilePaths(metricsfilepath);
			job.jm.totalfilesize = job.jm.totalfilesize/MDCConstants.MB;
			job.jm.totalblocks = totalblockslocation.size();
			if(isignite) {
				getDnXref(totalblockslocation, false);
				sendDataToIgniteServer(totalblockslocation,((IgnitePipeline)mdsroots.iterator().next()).getHdfspath());
			}else if(isjgroups||!islocal&&!isyarn&&!ismesos){
				getDnXref(totalblockslocation, true);
				if(!pc.getUseglobaltaskexecutors()) {
					getTaskExecutors(totalblockslocation);
				}else {
					getTaskExecutorsGlobal();
				}
				getContainersBalanced(totalblockslocation);
				job.jm.nodes = nodeschoosen;
				job.jm.containersallocated = containers.stream().collect(Collectors.toMap(key->key, value->0d));
			}else if(islocal||isyarn||ismesos) {
				getDnXref(totalblockslocation, false);
			}
			job.noofpartitions = noofpartition;
			if (job.stageoutputmap != null) {
				job.stageoutputmap.putAll(stageoutputmap);
			} else {
				job.stageoutputmap = stageoutputmap;
			}
			job.allstageshostport = allstageshostport;
			log.debug("Partitioning of Blocks ended.");
		} catch (Exception ex) {
			destroyContainers();
			log.error(PipelineConstants.FILEBLOCKSPARTITIONINGERROR, ex);
			throw new PipelineException(PipelineConstants.FILEBLOCKSPARTITIONINGERROR, ex);
		}
	}

	@SuppressWarnings("rawtypes")
	protected void sendDataToIgniteServer(List<BlocksLocation> totalblockslocation, String hdfspath) throws Exception {
		var cfg = new IgniteConfiguration();

		// The node will be started as a client node.
		cfg.setClientMode(true);
		cfg.setDeploymentMode(DeploymentMode.CONTINUOUS);
		// Classes of custom Java logic will be transferred over the wire from
		// this app.
		cfg.setPeerClassLoadingEnabled(true);
		// Setting up an IP Finder to ensure the client can locate the servers.
		var ipFinder = new TcpDiscoveryMulticastIpFinder();
		ipFinder.setAddresses(Arrays.asList(pipelineconfig.getIgnitehp()));
		cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));
		var cc = new CacheConfiguration(MDCConstants.MDCCACHE);
		cc.setCacheMode(CacheMode.PARTITIONED);
		cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		cc.setBackups(Integer.parseInt(pipelineconfig.getIgnitebackup()));
		cfg.setCacheConfiguration(cc);
		// Starting the node
		var ignite = Ignition.start(cfg);
		IgniteCache<Object, byte[]> ignitecache = ignite.getOrCreateCache(MDCConstants.MDCCACHE);
		try (var hdfs = FileSystem.newInstance(new URI(hdfspath), new Configuration());) {
			for (var bsl : totalblockslocation) {
				job.input.add(bsl);
				var databytes = HdfsBlockReader.getBlockDataMR(bsl, hdfs);
				var baos = new ByteArrayOutputStream();
				var lzfos = new SnappyOutputStream(baos);
				lzfos.write(databytes);
				lzfos.flush();
				ignitecache.put(bsl, baos.toByteArray());
				lzfos.close();
			}
		}
		job.igcache = ignitecache;
		job.ignite = ignite;
		var computeservers = job.ignite.cluster().forServers();
		job.jm.containersallocated = computeservers.hostNames().stream().collect(Collectors.toMap(key->key, value->0d));
	}
	
	protected void destroyContainers() throws PipelineException {
		try {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().acquire();
			if (!Objects.isNull(job.nodes)) {
				var nodes = job.nodes;
				var contcontainerids = GlobalContainerAllocDealloc.getContainercontainerids();
				var chpcres = GlobalContainerAllocDealloc.getHportcrs();
				var deallocateall = true;
				if (!Objects.isNull(job.containers)) {
					for (String container : job.containers) {
						var cids = contcontainerids.get(container);
						cids.remove(job.containerid);
						if (cids.isEmpty()) {
							contcontainerids.remove(container);
							var dc = new DestroyContainer();
							dc.setContainerid(job.containerid);
							dc.setContainerhp(container);
							String node = GlobalContainerAllocDealloc.getContainernode().remove(container);
							Set<String> containers = GlobalContainerAllocDealloc.getNodecontainers().get(node);
							containers.remove(container);
							Utils.writeObject(node, dc);
							ContainerResources cr = chpcres.remove(container);
						} else {
							deallocateall = false;
						}
					}
				}
				if (deallocateall) {
					var dc = new DestroyContainers();
					dc.setContainerid(job.containerid);
					log.debug("Destroying Containers with id:" + job.containerid + " for the hosts: " + nodes);
					for (var node : nodes) {
						Utils.writeObject(node, dc);
					}
				}
			}
		}
		catch (Exception ex) {
			log.error(PipelineConstants.DESTROYCONTAINERERROR, ex);
			throw new PipelineException(PipelineConstants.DESTROYCONTAINERERROR, ex);
		}finally {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().release();
		}
	}
	
	protected List<Path> getFilePaths(String hdfspth,String folder) throws PipelineException {
		try {
			var fileStatus = hdfs.listStatus(new Path(hdfspth + folder));
			var paths = FileUtil.stat2Paths(fileStatus);
			return Arrays.asList(paths);
		} catch (Exception ex) {
			log.error(PipelineConstants.FILEPATHERROR, ex);
			throw new PipelineException(PipelineConstants.FILEPATHERROR, ex);
		}
	}
	
	protected List<BlocksLocation> getBlocks(boolean isblocksuserdefined,long blocksize) throws PipelineException {
		try {
			List<BlocksLocation> bls = null;
					
			if(isblocksuserdefined) {		
				bls = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths,isblocksuserdefined, blocksize);
			} else {
				bls = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths,false,128*MDCConstants.MB);
			}
			return bls;
		} catch (Exception ex) {
			log.error(PipelineConstants.FILEBLOCKSERROR, ex);
			throw new PipelineException(PipelineConstants.FILEBLOCKSERROR, ex);
		}
	}
	
	protected void getContainersBalanced(List<BlocksLocation> bls) throws PipelineException {
		log.debug("Entered FileBlocksPartitionerHDFS.getContainersBalanced");
		var hostcontainermap = containers.stream()
				.collect(Collectors.groupingBy(key->key.split(MDCConstants.UNDERSCORE)[0],
						Collectors.mapping(container->container, 
								Collectors.toCollection(ArrayList::new))));
		var containerallocatecount = (Map<String, Long>) containers.stream().parallel().collect(Collectors.toMap(container -> container, container -> 0l));
		List<String> hostportcontainer;
		for (var b : bls) {
			hostportcontainer = hostcontainermap.get(b.block[0].hp.split(MDCConstants.COLON)[0]);
			if(Objects.isNull(hostportcontainer)) {
				throw new PipelineException(PipelineConstants.INSUFFNODESFORDATANODEERROR.replace("%s", b.block[0].hp).replace("%d", hostcontainermap.toString()));
			}
			var optional = hostportcontainer.stream().sorted((xref1, xref2) -> {
						return containerallocatecount.get(xref1).compareTo(containerallocatecount.get(xref2));
					}).findFirst();
			if(optional.isPresent()) {
				var container = optional.get();
				b.executorhp = container;
				containerallocatecount.put(container, containerallocatecount.get(container)+1);
			}else {
				throw new PipelineException(PipelineConstants.CONTAINERALLOCATIONERROR);
			}
		}
		log.debug("Exiting FileBlocksPartitionerHDFS.getContainersBalanced");
	}

	public void getDnXref(List<BlocksLocation> bls, boolean issa) throws PipelineException {
		log.debug("Entered FileBlocksPartitionerHDFS.getDnXref");
		var dnxrefs = bls.stream().parallel().flatMap(bl -> {
			var xrefs = new LinkedHashSet<String>();
			Iterator<Set<String>> xref = bl.block[0].dnxref.values().iterator();
			for (; xref.hasNext();) {
				xrefs.addAll(xref.next());
			}
			if (bl.block.length > 1 && !Objects.isNull(bl.block[1])) {
				xref = bl.block[0].dnxref.values().iterator();
				for (; xref.hasNext();) {
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
				var xrefselected = b.block[0].dnxref.keySet().stream()
						.filter(xrefhost -> computingnodes.contains(xrefhost))
						.flatMap(xrefhost -> b.block[0].dnxref.get(xrefhost).stream()).sorted((xref1, xref2) -> {
							return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
						}).findFirst();
				if (xrefselected.isEmpty()) {
					throw new PipelineException(
							PipelineConstants.INSUFFNODESERROR + " Available computing nodes are "
									+ computingnodes + " Available Data Nodes are " + b.block[0].dnxref.keySet());
				}
				final var xref = xrefselected.get();
				dnxrefallocatecount.put(xref, dnxrefallocatecount.get(xref) + 1);
				b.block[0].hp = xref;
				if (b.block.length > 1 && !Objects.isNull(b.block[1])) {
					xrefselected = b.block[1].dnxref.keySet().stream()
							.flatMap(xrefhost -> b.block[1].dnxref.get(xrefhost).stream())
							.filter(xrefhp -> xrefhp.split(MDCConstants.COLON)[0]
									.equals(xref.split(MDCConstants.COLON)[0]))
							.findFirst();
					if (xrefselected.isEmpty()) {
						xrefselected = b.block[1].dnxref.keySet().stream()
								.flatMap(xrefhost -> b.block[1].dnxref.get(xrefhost).stream()).findFirst();
						if (xrefselected.isEmpty()) {
							throw new PipelineException(PipelineConstants.INSUFFNODESERROR
									+ " Available computing nodes are " + computingnodes + " Available Data Nodes are "
									+ b.block[1].dnxref.keySet());
						}
					}
					var xref1 = xrefselected.get();
					b.block[1].hp = xref1;
				}
			}
		} else {
			for (var b : bls) {
				var xrefselected = b.block[0].dnxref.keySet().stream()
						.flatMap(xrefhost -> b.block[0].dnxref.get(xrefhost).stream()).sorted((xref1, xref2) -> {
							return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
						}).findFirst();
				var xref = xrefselected.get();
				dnxrefallocatecount.put(xref, dnxrefallocatecount.get(xref) + 1);
				b.block[0].hp = xref;
				if (b.block.length > 1 && !Objects.isNull(b.block[1])) {
					xrefselected = b.block[1].dnxref.keySet().stream()
							.flatMap(xrefhost -> b.block[1].dnxref.get(xrefhost).stream()).sorted((xref1, xref2) -> {
								return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
							}).findFirst();
					xref = xrefselected.get();
					b.block[1].hp = xref;
				}
			}
		}
		log.debug("Exiting FileBlocksPartitionerHDFS.getDnXref");
	}
	
	public List<ContainerResources> getTotalMemoryContainersReuseAllocation(String nodehp, long totalmemorytoalloc, AtomicLong totalallocated) {
		var containers = GlobalContainerAllocDealloc.getNodecontainers().get(nodehp);
		var cres = new ArrayList<ContainerResources>();
		if(!Objects.isNull(containers)) {
			long memtotal = 0;
			for(String container:containers) {
				ContainerResources crs = GlobalContainerAllocDealloc.getHportcrs().get(container);
				if(!Objects.isNull(crs)) {
					cres.add(crs);
					memtotal+=crs.getMaxmemory();
				}
				if(memtotal>=totalmemorytoalloc) {
					break;
				}
			}
			totalallocated.set(memtotal);
		}
		return cres;
	}
	
	protected void getTaskExecutors(List<BlocksLocation> bls) throws PipelineException {
		try {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().acquire();
			var containerid = MDCConstants.CONTAINER+MDCConstants.HYPHEN+Utils.getUniqueID();
			job.containerid = containerid;
			containers = new ArrayList<>();
			nodeschoosen = new HashSet<>();
			var loadjar = new LoadJar();
			loadjar.mrjar = pipelineconfig.getJar();
			var totalcontainersallocated = 0;
			var nodestotalblockmem = new ConcurrentHashMap<String,Long>();
			getNodesResourcesSorted(bls,nodestotalblockmem);
			job.lcs = new ArrayList<>();
			for (var te : nodessorted) {
				var host = te.split("_")[0];
				var lc = new LaunchContainers();
				lc.setNodehostport(te);
				lc.setContainerid(containerid);
				lc.setJobid(job.id);
				lc.setMode(isignite?LaunchContainers.MODE.IGNITE:LaunchContainers.MODE.NORMAL);
				var cla = new ContainerLaunchAttributes();
				AtomicLong totalallocated =  new AtomicLong();
				var cr = getTotalMemoryContainersReuseAllocation(te, nodestotalblockmem.get(host),totalallocated);
				long totalallocatedremaining = nodestotalblockmem.get(host) - totalallocated.get();
				List<ContainerResources> contres = null;
				if(totalallocatedremaining > 0 && cr.isEmpty()) {
					if(Objects.isNull(resources.get(te))) {
						throw new PipelineException(PipelineConstants.RESOURCESDOWNRESUBMIT.replace("%s", te));
					}
					contres=getNumberOfContainers(pipelineconfig.getGctype(),nodestotalblockmem.get(host),
							resources.get(te));
				}
				job.lcs.add(lc);
				ports = null;
				if(!Objects.isNull(contres)&&!contres.isEmpty()) {
					cla.setNumberofcontainers(contres.size());
					cla.setCr(contres);
					lc.setCla(cla);
					var ac = new AllocateContainers();
					ac.setContainerid(containerid);
					ac.setNumberofcontainers(contres.size());
					ports = (List<Integer>) Utils.getResultObjectByInput(te, ac);					
				}
				if(!Objects.isNull(cr)) {
					if(Objects.isNull(ports)) {
						ports = new ArrayList<>();
					}
					for(ContainerResources r:cr) {						
						ports.add(r.getPort());
					}
					if(!Objects.isNull(contres)) {
						contres.addAll(cr);
						cla.setNumberofcontainers(contres.size());
						cla.setCr(contres);
						lc.setCla(cla);
					}else {
						contres = cr;
						cla.setNumberofcontainers(cr.size());
						cla.setCr(cr);
						lc.setCla(cla);
					
					}
				}
				if(Objects.isNull(ports)||ports.isEmpty()) {
					continue;
				}
				for(int containercount=0;containercount<ports.size();containercount++) {
					ContainerResources crs = contres.get(containercount);
					crs.setPort(ports.get(containercount));
					String conthp = host + MDCConstants.UNDERSCORE + ports.get(containercount);
					containers.add(conthp);
					var containerids = GlobalContainerAllocDealloc.getContainercontainerids().get(conthp);
					if(Objects.isNull(containerids)) {
						containerids = new ArrayList<>();
						GlobalContainerAllocDealloc.getContainercontainerids().put(conthp, containerids);
					}
					containerids.add(containerid);
					GlobalContainerAllocDealloc.getHportcrs().put(conthp, crs);
					GlobalContainerAllocDealloc.getContainernode().put(conthp, te);
				}
				Set<String> contallocated = GlobalContainerAllocDealloc.getNodecontainers().get(te);
				if(Objects.isNull(contallocated)) {
					contallocated = new LinkedHashSet<>();
					GlobalContainerAllocDealloc.getNodecontainers().put(te, contallocated);
				}
				contallocated.addAll(containers);			
				totalcontainersallocated += contres.size();
				nodeschoosen.add(te);
			}
			job.containers = containers;
			job.nodes = nodeschoosen;
			job.jm.containerresources = job.lcs.stream().flatMap(lc -> {
				var crs = lc.getCla().getCr();
				return crs.stream().map(cr -> {
					var node = lc.getNodehostport().split(MDCConstants.UNDERSCORE)[0];
					var cpu = cr.getCpu();
					var maxmemory = cr.getMaxmemory();
					var port = cr.getPort();
					return MDCConstants.BR + node + MDCConstants.UNDERSCORE + port + MDCConstants.COLON + MDCConstants.BR + MDCConstants.CPUS
							+ MDCConstants.EQUAL + cpu + MDCConstants.BR + MDCConstants.MEM + MDCConstants.EQUAL
							+ maxmemory;

				}).collect(Collectors.toList()).stream();
			}).collect(Collectors.toList());
			log.debug("Total Containers Allocated:"	+ totalcontainersallocated);
		} catch (Exception ex) {
			log.error(PipelineConstants.TASKEXECUTORSALLOCATIONERROR, ex);
			throw new PipelineException(PipelineConstants.TASKEXECUTORSALLOCATIONERROR, ex);
		}finally {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().release();
		}
	}

	
	protected void getTaskExecutorsGlobal() {
		job.lcs = GlobalContainerLaunchers.getAll();
		job.containerid = job.lcs.get(0).getContainerid();
		containers = job.containers = job.lcs.stream().flatMap(lc->{
			var host = lc.getNodehostport().split(MDCConstants.UNDERSCORE);
			return lc.getCla().getCr().stream().map(cr->{
				return host[0]+MDCConstants.UNDERSCORE+cr.getPort();
			}
			).collect(Collectors.toList()).stream();
		}).collect(Collectors.toList());
		job.nodes = job.lcs.stream().map(lc->lc.getNodehostport()).collect(Collectors.toSet());
	}
	
	protected void getNodesResourcesSorted(List<BlocksLocation> bls,Map<String,Long> nodestotalblockmem) {
		resources = MDCNodesResources.get();
		
		var nodeswithhostonly = bls.stream().flatMap(bl -> {
			var block1 = bl.block[0];
			Block block2 = null;
			if(bl.block.length>1) {
				block2 = bl.block[1];
			}
			var xref = new HashSet<String>();
			if (!Objects.isNull(block1)) {
				xref.add(block1.hp.split(MDCConstants.COLON)[0]);
				var value = nodestotalblockmem.get(block1.hp.split(MDCConstants.COLON)[0]);
				if(value!=null){
					nodestotalblockmem.put(block1.hp.split(MDCConstants.COLON)[0],value+(block1.blockend-block1.blockstart));
				}else {
					nodestotalblockmem.put(block1.hp.split(MDCConstants.COLON)[0],block1.blockend-block1.blockstart);
				}
			}
			if (!Objects.isNull(block2)) {
				xref.add(block2.hp.split(MDCConstants.COLON)[0]);
				var value = nodestotalblockmem.get(block2.hp.split(MDCConstants.COLON)[0]);
				if(value!=null){
					nodestotalblockmem.put(block2.hp.split(MDCConstants.COLON)[0],value+(block2.blockend-block2.blockstart));
				}else {
					nodestotalblockmem.put(block2.hp.split(MDCConstants.COLON)[0],block2.blockend-block2.blockstart);
				}
			}
			return xref.stream();
		}).collect(Collectors.toSet());
		nodessorted = resources.entrySet().stream().sorted((entry1, entry2) -> {
			var r1 = entry1.getValue();
			var r2 = entry2.getValue();
			if (r1.getNumberofprocessors() < r2.getNumberofprocessors()) {
				return -1;
			} else if(r1.getNumberofprocessors() == r2.getNumberofprocessors()) {
				if (r1.getFreememory() < r2.getFreememory()) {
					return -1;
				} else if(r1.getFreememory() == r2.getFreememory()) {
					return 0;
				}
				else {
					return 1;
				}
			}else {
				return 1;
			}
		}).map(entry -> entry.getKey())
				.filter(key->nodeswithhostonly.contains(key.split(MDCConstants.UNDERSCORE)[0]))
				.collect(Collectors.toList());
	}

	protected List<ContainerResources> getNumberOfContainers(String gctype, long totalmem, Resources resources)
			throws PipelineException {
		var cpu = resources.getNumberofprocessors() - 1;
		var cr = new ArrayList<ContainerResources>();
		if(pipelineconfig.getContaineralloc().equals(MDCConstants.CONTAINER_ALLOC_DEFAULT)) {
			var res = new ContainerResources();
			var actualmemory = (resources.getFreememory()-256*MDCConstants.MB);
			if (actualmemory < (128 * MDCConstants.MB)) {
				throw new PipelineException(PipelineConstants.MEMORYALLOCATIONERROR);
			}
			if (totalmem < (512 * MDCConstants.MB) && totalmem > (0) && cpu >= 1) {
				if (actualmemory >= totalmem) {
					res.setCpu(1);
					var heapmem = 1024*Integer.valueOf(pipelineconfig.getHeappercent())/100;
					res.setMinmemory(heapmem);
					res.setMaxmemory(heapmem);
					res.setDirectheap(1024-heapmem);
					res.setGctype(gctype);
					cr.add(res);
					return cr;
				} else {
					throw new PipelineException(PipelineConstants.INSUFFMEMORYALLOCATIONERROR);
				}
			}
			res.setCpu(cpu);
			var memoryrequire = totalmem < actualmemory?totalmem:actualmemory;
			var meminmb = memoryrequire/MDCConstants.MB;
			var heapmem = meminmb*Integer.valueOf(pipelineconfig.getHeappercent())/100;
			res.setMinmemory(heapmem);
			res.setMaxmemory(heapmem);
			res.setDirectheap(meminmb-heapmem);
			res.setGctype(gctype);
			cr.add(res);
			return cr;
		}else if(pipelineconfig.getContaineralloc().equals(MDCConstants.CONTAINER_ALLOC_DIVIDED)){
			var actualmemory = (resources.getFreememory()-256*MDCConstants.MB);
			if (actualmemory < (128 * MDCConstants.MB)) {
				throw new PipelineException(PipelineConstants.MEMORYALLOCATIONERROR);
			}
			if (totalmem < (512 * MDCConstants.MB) && totalmem > (0) && cpu >= 1) {
				if (actualmemory >= totalmem) {
					var res = new ContainerResources();
					res.setCpu(1);
					var heapmem = 1024*Integer.valueOf(pipelineconfig.getHeappercent())/100;
					res.setMinmemory(heapmem);
					res.setMaxmemory(heapmem);
					res.setDirectheap(1024-heapmem);
					res.setGctype(gctype);
					cr.add(res);
					return cr;
				} else {
					throw new PipelineException(PipelineConstants.INSUFFMEMORYALLOCATIONERROR);
				}
			}
			if (cpu == 0)
				return cr;
			var numofcontainerspermachine = Integer.parseInt(pipelineconfig.getNumberofcontainers());
			var dividedcpus = cpu/numofcontainerspermachine;
			var maxmemory = actualmemory / numofcontainerspermachine;
			var maxmemmb = maxmemory / MDCConstants.MB;
			var totalmemmb = totalmem / MDCConstants.MB;
			if(dividedcpus==0 && cpu>=1) {
				dividedcpus = 1;
			}
			if (totalmem < maxmemory && dividedcpus >= 1) {
				var res = new ContainerResources();
				res.setCpu(dividedcpus);
				var heapmem = totalmemmb*Integer.valueOf(pipelineconfig.getHeappercent())/100;
				res.setMinmemory(heapmem);
				res.setMaxmemory(heapmem);
				res.setDirectheap(totalmemmb-heapmem);
				res.setGctype(gctype);
				cr.add(res);
				return cr;
			}
			var numberofcontainer = 0;
			while (true) {
				if (cpu >= dividedcpus && totalmem >= 0) {
					var res = new ContainerResources();
					res.setCpu(dividedcpus);
					var heapmem = maxmemmb*Integer.valueOf(pipelineconfig.getHeappercent())/100;
					res.setMinmemory(heapmem);
					res.setMaxmemory(heapmem);
					res.setDirectheap(maxmemmb-heapmem);
					res.setGctype(gctype);
					cr.add(res);
				} else if (cpu >= 1 && totalmem >= 0) {
					var res = new ContainerResources();
					res.setCpu(cpu);
					var heapmem = maxmemmb*Integer.valueOf(pipelineconfig.getHeappercent())/100;
					res.setMinmemory(heapmem);
					res.setMaxmemory(heapmem);
					res.setDirectheap(maxmemmb-heapmem);
					res.setGctype(gctype);
					cr.add(res);
				}else {
					break;
				}
				numberofcontainer++;
				if(numofcontainerspermachine==numberofcontainer)break;
				cpu-=dividedcpus;
				totalmem -= maxmemory;
			}
			return cr;
		}
		else {
			throw new PipelineException(PipelineConstants.UNSUPPORTEDMEMORYALLOCATIONMODE);
		}
	}
}
