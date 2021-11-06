package com.github.mdc.stream.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.ContainerResources;
import com.github.mdc.common.GlobalContainerAllocDealloc;
import com.github.mdc.common.Job;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCNodesResources;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.PipelineConstants;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Resources;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.StreamPipelineBase;
import com.github.mdc.stream.utils.FileBlocksPartitionerHDFS;
import com.github.mdc.tasks.executor.NodeRunner;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FileBlocksPartitionerHDFSTest extends StreamPipelineBase{
	private static final int NOOFNODES = 1;
	static int teport = 12121;
	static ExecutorService es,escontainer;
	static ConcurrentMap<String, List<ServerSocket>> containers;
	static ConcurrentMap<String, List<Thread>> tes;
	static ServerSocket ss;
	static List<ServerSocket> containerlauncher = new ArrayList<>();
	static Logger log = Logger.getLogger(FileBlocksPartitionerHDFSTest.class);
	@BeforeClass
	public static void launchNodes() throws Exception {
		Utils.loadLog4JSystemPropertiesClassPath("mdctest.properties");
		containers = new ConcurrentHashMap<>();
		tes = new ConcurrentHashMap<>();
		es = Executors.newWorkStealingPool();
		Semaphore semaphore = new Semaphore(1);
		CountDownLatch cdl = new CountDownLatch(NOOFNODES);
		AtomicInteger portinc = new AtomicInteger(teport);
		escontainer = Executors.newWorkStealingPool();
		var containerprocesses = new ConcurrentHashMap<String, Map<String,Process>>();
		var containeridthreads = new ConcurrentHashMap<String, Map<String,List<Thread>>>();
		var containeridports = new ConcurrentHashMap<String, List<Integer>>();
		for(int nodeindex = 0;nodeindex<NOOFNODES;nodeindex++) {
			semaphore.acquire();
			ss = new ServerSocket(20000+nodeindex,256,InetAddress.getByAddress(new byte[] { 0x00, 0x00, 0x00, 0x00 }));
			containerlauncher.add(ss);
			
			es.execute(()->{
				cdl.countDown();
				semaphore.release();
				while(true) {
					try(Socket sock = ss.accept();) {
						var container = new NodeRunner(sock, portinc, MDCConstants.PROPLOADERCONFIGFOLDER,
								containerprocesses, hdfs, containeridthreads,containeridports);
						Future<Boolean> containerallocated = escontainer.submit(container);
						log.info("Containers Allocated: "+containerallocated.get()+" Next Port Allocation:"+portinc.get());
					} catch (Exception e) {
					}
				}
			});
		}
		cdl.await();
	}
	
	@AfterClass
	public static void shutdownNodes() throws Exception {
		containers.keySet().stream().flatMap(key->containers.get(key).stream()).forEach(servers->{
			try {
				servers.close();
			} catch (IOException e) {
			}
		});
		tes.keySet().stream().flatMap(key->tes.get(key).stream()).forEach(thr->thr.stop());
		if(!Objects.isNull(containerlauncher)) {
			containerlauncher.stream().forEach(ss->{
				try {
					ss.close();
				} catch (IOException e) {				}
			});
		}
		if(!Objects.isNull(es)) {
			es.shutdown();
		}
		if(!Objects.isNull(escontainer)) {
			escontainer.shutdown();
		}
	}
	
	
	@Test
	public void testGetContainersBalanced() throws Exception {
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.supplier = ()->2;
		FileSystem hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
		fbp.hdfs = hdfs;
		FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airlinesample));
		Path[] paths = FileUtil.stat2Paths(fileStatus);
		fbp.filepaths = Arrays.asList(paths);
		fbp.isblocksuserdefined = true;
		fbp.isyarn = false;
		fbp.ismesos = false;
		fbp.islocal = false;
		fbp.isjgroups = false;
		fbp.isignite = false;
		fbp.nodeschoosen = new HashSet<>(Arrays.asList("127.0.0.1_20000"));
		fbp.containers = Arrays.asList(MDCConstants.DUMMYCONTAINER);
		List<BlocksLocation> bls = fbp.getHDFSParitions();
		assertEquals(2,bls.size());
		assertEquals(4270834, fbp.totallength);
		fbp.getContainersBalanced(bls);
		assertEquals("127.0.0.1_10101",bls.get(0).executorhp);
		assertEquals("127.0.0.1_10101",bls.get(1).executorhp);
	}
	
	@Test
	public void testGetContainersBalancedMultipleContainer() throws Exception {
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.supplier = ()->2;
		FileSystem hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
		fbp.hdfs = hdfs;
		FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airlinesample));
		Path[] paths = FileUtil.stat2Paths(fileStatus);
		fbp.filepaths = Arrays.asList(paths);
		fbp.isblocksuserdefined = true;
		fbp.isyarn = false;
		fbp.ismesos = false;
		fbp.islocal = false;
		fbp.isjgroups = false;
		fbp.isignite = false;
		fbp.nodeschoosen = new HashSet<>(Arrays.asList("127.0.0.1_20000"));
		fbp.containers = Arrays.asList(MDCConstants.DUMMYCONTAINER, "127.0.0.1_10102");
		List<BlocksLocation> bls = fbp.getHDFSParitions();
		assertEquals(2,bls.size());
		assertEquals(4270834, fbp.totallength);
		fbp.getContainersBalanced(bls);
		assertEquals("127.0.0.1_10101",bls.get(0).executorhp);
		assertEquals("127.0.0.1_10102",bls.get(1).executorhp);
	}
	
	@Test
	public void testGetHDFSParitions() throws Exception {
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.supplier = ()->2;
		FileSystem hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
		fbp.hdfs = hdfs;
		FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airlinesample));
		Path[] paths = FileUtil.stat2Paths(fileStatus);
		fbp.filepaths = Arrays.asList(paths);
		fbp.isblocksuserdefined = true;
		fbp.isyarn = false;
		fbp.ismesos = false;
		fbp.islocal = false;
		fbp.isjgroups = false;
		fbp.isignite = false;
		fbp.nodeschoosen = new HashSet<>(Arrays.asList("127.0.0.1_20000"));
		fbp.containers = Arrays.asList(MDCConstants.DUMMYCONTAINER);
		List<BlocksLocation> bls = fbp.getHDFSParitions();
		assertEquals(2,bls.size());
		assertEquals(4270834, fbp.totallength);
	}
	
	@Test
	public void testGetNodesResourcesSortedAuto() throws Exception {
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		FileSystem hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
		fbp.hdfs = hdfs;
		FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airlinesample));
		Path[] paths = FileUtil.stat2Paths(fileStatus);
		fbp.filepaths = Arrays.asList(paths);
		fbp.isblocksuserdefined = false;
		List<BlocksLocation> bls = fbp.getBlocks(fbp.isblocksuserdefined, 128);
		ConcurrentMap<String, Resources> noderesourcesmap = new ConcurrentHashMap<>();
		Resources resource = new Resources();
		resource.setFreememory(12*1024*1024*1024l);
		resource.setNumberofprocessors(4);
		noderesourcesmap.put("127.0.0.1_20000", resource);
		resource = new Resources();
		resource.setFreememory(6*1024*1024*1024l);
		resource.setNumberofprocessors(4);
		noderesourcesmap.put("127.0.0.1_20001", resource);
		MDCNodesResources.put(noderesourcesmap);
		Map<String,Long> nodestotalblockmem = new ConcurrentHashMap<>();
		fbp.getDnXref(bls, false);
		fbp.getNodesResourcesSorted(bls,nodestotalblockmem);
		assertEquals(2,fbp.nodessorted.size());
		assertEquals("127.0.0.1_20001",fbp.nodessorted.get(0));
		assertEquals("127.0.0.1_20000",fbp.nodessorted.get(1));
	}
	
	
	@Test
	public void testGetNumberOfContainersAuto() throws Exception {
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		FileSystem hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
		fbp.hdfs = hdfs;
		FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airlinesample));
		Path[] paths = FileUtil.stat2Paths(fileStatus);
		fbp.filepaths = Arrays.asList(paths);
		fbp.isblocksuserdefined = false;
		Resources resources = new Resources();
		resources.setFreememory(12*1024*1024*1024l);
		resources.setNumberofprocessors(4);
		List<ContainerResources> crs = fbp.getNumberOfContainers(MDCConstants.GCCONFIG_DEFAULT,20,resources);
		assertEquals(1, crs.size());
		assertEquals(128, crs.get(0).getMaxmemory());
		assertEquals(128, crs.get(0).getMinmemory());
		assertEquals(1, crs.get(0).getCpu());
	}
	
	
	@Test
	public void testGetTaskExecutorsAuto() throws Exception {
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		FileSystem hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
		fbp.hdfs = hdfs;
		FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airlinesample));
		Path[] paths = FileUtil.stat2Paths(fileStatus);
		fbp.filepaths = Arrays.asList(paths);
		fbp.isblocksuserdefined = false;
		List<BlocksLocation> bls = fbp.getBlocks(fbp.isblocksuserdefined, 128);
		ConcurrentMap<String, Resources> noderesourcesmap = new ConcurrentHashMap<>();
		Resources resource = new Resources();
		resource.setFreememory(12*1024*1024*1024l);
		resource.setNumberofprocessors(4);
		noderesourcesmap.put("127.0.0.1_20000", resource);
		MDCNodesResources.put(noderesourcesmap);
		fbp.pipelineconfig = new PipelineConfig();
		fbp.pipelineconfig.setMaxmem("4096");
		fbp.pipelineconfig.setNumberofcontainers("5");
		fbp.job = new Job();
		fbp.isignite = false;
		fbp.getDnXref(bls, false);
		fbp.getTaskExecutors(bls);
		assertEquals(1,fbp.job.containers.size());
		assertEquals(1,fbp.job.nodes.size());
		fbp.destroyContainers();
		GlobalContainerAllocDealloc.getHportcrs().clear();
	}
	
	@Test
	public void testGetTaskExecutorsLessResourcesInputCpu() throws Exception {
		ConcurrentMap<String, Resources> noderesourcesmap = new ConcurrentHashMap<>();
		Resources resource = new Resources();
		resource.setFreememory(12*1024*1024*1024l);
		resource.setNumberofprocessors(1);
		noderesourcesmap.put("127.0.0.1_20000", resource);
		MDCNodesResources.put(noderesourcesmap);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.pipelineconfig = new PipelineConfig();
		fbp.pipelineconfig.setMaxmem("4096");
		fbp.pipelineconfig.setNumberofcontainers("5");
		fbp.job = new Job();
		fbp.isignite = false;
		FileSystem hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
		fbp.hdfs = hdfs;
		FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airlinesample));
		Path[] paths = FileUtil.stat2Paths(fileStatus);
		fbp.filepaths = Arrays.asList(paths);
		fbp.isblocksuserdefined = false;
		fbp.hdfs = hdfs;
		List<BlocksLocation> bls = fbp.getBlocks(fbp.isblocksuserdefined,128);
		fbp.getDnXref(bls, false);
		fbp.getTaskExecutors(bls);
		assertEquals(0,fbp.job.containers.size());
		assertEquals(0,fbp.job.nodes.size());
		fbp.destroyContainers();
		GlobalContainerAllocDealloc.getHportcrs().clear();
	}

	@Test
	public void testGetTaskExecutorsLessResourcesInputMemory1() throws Exception {
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		try {
			ConcurrentMap<String, Resources> noderesourcesmap = new ConcurrentHashMap<>();
			Resources resource = new Resources();
			resource.setFreememory(1 * 1024 * 1024l);
			resource.setNumberofprocessors(4);
			noderesourcesmap.put("127.0.0.1_20000", resource);
			MDCNodesResources.put(noderesourcesmap);
			fbp.pipelineconfig = new PipelineConfig();
			fbp.pipelineconfig.setMaxmem("4096");
			fbp.pipelineconfig.setNumberofcontainers("5");
			fbp.isignite = false;
			fbp.job = new Job();
			FileSystem hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
			fbp.hdfs = hdfs;
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airlinesample));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			fbp.isblocksuserdefined = false;
			fbp.hdfs = hdfs;
			fbp.filepaths = Arrays.asList(paths);
			List<BlocksLocation> bls = fbp.getBlocks(fbp.isblocksuserdefined, 128);
			fbp.getDnXref(bls, false);
			fbp.getTaskExecutors(bls);			
		} catch (Exception ex) {
			assertEquals(PipelineConstants.MEMORYALLOCATIONERROR, ex.getCause().getMessage());
			assertNull(fbp.job.containers);
			assertNull(fbp.job.nodes);
		} finally {
			fbp.destroyContainers();
			GlobalContainerAllocDealloc.getHportcrs().clear();
		}
	}
	@Test
	public void testGetTaskExecutorsLessResourcesInputMemory2() throws Exception {
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		try {
			ConcurrentMap<String, Resources> noderesourcesmap = new ConcurrentHashMap<>();
			Resources resource = new Resources();
			resource.setFreememory(400 * 1024 * 1024l+128*MDCConstants.MB*3* Integer.parseInt(MDCProperties
					.get().getProperty(MDCConstants.BYTEBUFFERPOOL_MAX, MDCConstants.BYTEBUFFERPOOL_MAX_DEFAULT)));
			resource.setNumberofprocessors(4);
			noderesourcesmap.put("127.0.0.1_20000", resource);
			MDCNodesResources.put(noderesourcesmap);
			fbp.pipelineconfig = new PipelineConfig();
			fbp.pipelineconfig.setMaxmem("4096");
			fbp.pipelineconfig.setNumberofcontainers("5");
			fbp.isignite = false;
			fbp.job = new Job();
			FileSystem hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
			fbp.hdfs = hdfs;
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airline1989));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			fbp.isblocksuserdefined = false;
			fbp.hdfs = hdfs;
			fbp.filepaths = Arrays.asList(paths);
			List<BlocksLocation> bls = fbp.getBlocks(fbp.isblocksuserdefined, 128);
			fbp.getDnXref(bls, false);
			fbp.getTaskExecutors(bls);			
		} catch (Exception ex) {
			assertEquals(PipelineConstants.INSUFFMEMORYALLOCATIONERROR, ex.getCause().getMessage());
			assertNull(fbp.job.containers);
			assertNull(fbp.job.nodes);
		} finally {
			fbp.destroyContainers();
			GlobalContainerAllocDealloc.getHportcrs().clear();
		}
	}
	@Test
	public void testGetTaskExecutorsProperInput() throws Exception {
		ConcurrentMap<String, Resources> noderesourcesmap = new ConcurrentHashMap<>();
		Resources resource = new Resources();
		resource.setFreememory(12*1024*1024*1024l);
		resource.setNumberofprocessors(4);
		noderesourcesmap.put("127.0.0.1_20000", resource);
		MDCNodesResources.put(noderesourcesmap);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.pipelineconfig = new PipelineConfig();
		fbp.pipelineconfig.setMaxmem("4096");
		fbp.pipelineconfig.setNumberofcontainers("5");
		fbp.pipelineconfig.setBlocksize("128");
		fbp.job = new Job();
		fbp.isignite = false;
		FileSystem hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
		fbp.hdfs = hdfs;
		FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airlinesample));
		Path[] paths = FileUtil.stat2Paths(fileStatus);
		fbp.hdfs = hdfs;
		fbp.isblocksuserdefined = true;
		fbp.filepaths = Arrays.asList(paths);
		List<BlocksLocation> bls = fbp.getBlocks(fbp.isblocksuserdefined,128*MDCConstants.MB);
		fbp.getDnXref(bls, false);
		fbp.getTaskExecutors(bls);
		assertEquals(1,fbp.job.containers.size());
		assertEquals("127.0.0.1_12122",fbp.job.containers.get(0));
		assertEquals("127.0.0.1_20000",fbp.job.nodes.iterator().next());
		assertEquals(1,fbp.job.nodes.size());
		fbp.destroyContainers();
		GlobalContainerAllocDealloc.getHportcrs().clear();
	}
}
