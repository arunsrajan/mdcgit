package com.github.mdc.stream.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.github.mdc.common.GlobalContainerAllocDealloc;
import com.github.mdc.common.Job;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCNodesResources;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Resources;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.StreamPipelineBase;
import com.github.mdc.stream.utils.FileBlocksPartitionerHDFS;
import com.github.mdc.tasks.executor.NodeRunner;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FileBlocksPartitionerHDFSMultipleNodesTest extends StreamPipelineBase{
	private static final int NOOFNODES = 5;
	static int teport = 12121;
	static ExecutorService es,escontainer;
	static ConcurrentMap<String, List<ServerSocket>> containers;
	static ConcurrentMap<String, List<Thread>> tes;	
	static List<ServerSocket> containerlauncher = new ArrayList<>();
	static Logger log = Logger.getLogger(FileBlocksPartitionerHDFSTest.class);
	static int nodeindex = 0;
	static FileSystem hdfs;
	static Path[] paths;
	static List<BlocksLocation> bls;
	@BeforeClass
	public static void launchNodes() throws Exception {
		Utils.loadLog4JSystemPropertiesClassPath("mdctest.properties");
		hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());		
		FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airlines));
		paths = FileUtil.stat2Paths(fileStatus);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.hdfs = hdfs;
		fbp.filepaths = Arrays.asList(paths);
		bls = fbp.getBlocks(true,128*MDCConstants.MB);
		containers = new ConcurrentHashMap<>();
		tes = new ConcurrentHashMap<>();
		es = Executors.newFixedThreadPool(NOOFNODES);	
		Semaphore semaphore = new Semaphore(1);
		CountDownLatch cdl = new CountDownLatch(NOOFNODES);		
		escontainer = Executors.newFixedThreadPool(100);
		var containerprocesses = new ConcurrentHashMap<String, Map<String,Process>>();
		var containeridthreads = new ConcurrentHashMap<String, Map<String,List<Thread>>>();
		var containeridports = new ConcurrentHashMap<String, List<Integer>>();
		ConcurrentMap<String, Resources> noderesourcesmap = new ConcurrentHashMap<>();
		MDCNodesResources.put(noderesourcesmap);
		for(;nodeindex<NOOFNODES;nodeindex++) {
			semaphore.acquire();
			ServerSocket ss = new ServerSocket(20000+nodeindex,256,InetAddress.getByAddress(new byte[] { 0x00, 0x00, 0x00, 0x00 }));
			containerlauncher.add(ss);
			Resources resource = new Resources();
			int memory = 64;
			resource.setFreememory(memory*1024*1024*1024l);
			resource.setNumberofprocessors(4);
			noderesourcesmap.put("127.0.0.1_"+(20000+nodeindex), resource);
			es.execute(()->{
				ServerSocket ssl = ss;
				cdl.countDown();
				AtomicInteger portinc = new AtomicInteger(teport+nodeindex*100);
				semaphore.release();				
				while(true) {
					try(Socket sock = ssl.accept();) {
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
	public void testGetNodesResourcesSortedAuto() throws Exception {
		log.info("FileBlocksPartitionerHDFSMultipleNodesTest.testGetNodesResourcesSortedAuto() Entered------------------------------");
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.hdfs = hdfs;		
		fbp.isblocksuserdefined = false;
		fbp.job = new Job();
		Map<String,Long> nodestotalblockmem = new ConcurrentHashMap<>();
		fbp.getDnXref(bls, false);
		fbp.getNodesResourcesSorted(bls,nodestotalblockmem);
		log.info(fbp.nodessorted);
		log.info("FileBlocksPartitionerHDFSMultipleNodesTest.testGetNodesResourcesSortedAuto() Exiting------------------------------");
	}
	
	
	@Test
	public void testGetTaskExecutorsAuto() throws Exception {
		log.info("FileBlocksPartitionerHDFSMultipleNodesTest.testGetTaskExecutorsAuto() Entered------------------------------");
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.hdfs = hdfs;
		fbp.filepaths = Arrays.asList(paths);
		fbp.isblocksuserdefined = false;
		fbp.pipelineconfig = new PipelineConfig();
		fbp.pipelineconfig.setMaxmem("4096");
		fbp.pipelineconfig.setNumberofcontainers("5");
		fbp.job = new Job();
		fbp.isignite = false;
		fbp.getDnXref(bls, false);
		fbp.getTaskExecutors(bls);
		log.info(fbp.job.nodes);
		log.info(fbp.job.containers);		
		fbp.destroyContainers();
		GlobalContainerAllocDealloc.getHportcrs().clear();
		log.info("FileBlocksPartitionerHDFSMultipleNodesTest.testGetTaskExecutorsAuto() Exiting------------------------------");
	}
	
	@Test
	public void testGetTaskExecutorsProperInput() throws Exception {
		log.info("FileBlocksPartitionerHDFSMultipleNodesTest.testGetTaskExecutorsProperInput() Entered------------------------------");
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.pipelineconfig = new PipelineConfig();
		fbp.pipelineconfig.setMaxmem("4096");
		fbp.pipelineconfig.setNumberofcontainers("5");
		fbp.job = new Job();
		fbp.isignite = false;
		fbp.filepaths = Arrays.asList(paths);
		fbp.isblocksuserdefined = false;
		fbp.hdfs = hdfs;
		fbp.getDnXref(bls, false);
		fbp.getTaskExecutors(bls);
		log.info(fbp.job.nodes);
		log.info(fbp.job.containers);
		fbp.destroyContainers();
		GlobalContainerAllocDealloc.getHportcrs().clear();
		log.info("FileBlocksPartitionerHDFSMultipleNodesTest.testGetTaskExecutorsProperInput() Exiting------------------------------");		
	}
}
