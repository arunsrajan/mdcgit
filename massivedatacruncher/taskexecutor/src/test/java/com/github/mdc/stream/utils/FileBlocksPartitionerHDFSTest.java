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
package com.github.mdc.stream.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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

import com.esotericsoftware.kryonetty.ServerEndpoint;
import com.esotericsoftware.kryonetty.network.ReceiveEvent;
import com.esotericsoftware.kryonetty.network.handler.NetworkHandler;
import com.esotericsoftware.kryonetty.network.handler.NetworkListener;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.GlobalContainerAllocDealloc;
import com.github.mdc.common.Job;
import com.github.mdc.common.JobMetrics;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCNodesResources;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.PipelineConstants;
import com.github.mdc.common.Resources;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.StreamPipelineBase;
import com.github.mdc.tasks.executor.NodeRunner;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FileBlocksPartitionerHDFSTest extends StreamPipelineBase {
	private static final int NOOFNODES = 1;
	static int teport = 12121;
	static ExecutorService es,escontainer;
	static ConcurrentMap<String, List<ServerSocket>> containers;
	static ConcurrentMap<String, List<Thread>> tes;
	static ServerSocket ss;
	static List<ServerEndpoint> containerlauncher = new ArrayList<>();
	static Logger log = Logger.getLogger(FileBlocksPartitionerHDFSTest.class);
	private static ServerEndpoint server;

	@BeforeClass
	public static void launchNodes() throws Exception {
		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, "mdctest.properties");
		containers = new ConcurrentHashMap<>();
		tes = new ConcurrentHashMap<>();
		es = Executors.newWorkStealingPool();
		escontainer = Executors.newWorkStealingPool();
		var containerprocesses = new ConcurrentHashMap<String, Map<String, Process>>();
		var containeridthreads = new ConcurrentHashMap<String, Map<String, List<Thread>>>();
		var containeridports = new ConcurrentHashMap<String, List<Integer>>();
		for (int nodeindex = 0; nodeindex < NOOFNODES; nodeindex++) {
			server = Utils.getServerKryoNetty(20000 + nodeindex,
					new NetworkListener() {
					@NetworkHandler
		            public void onReceive(ReceiveEvent event) {
						try {
							Object object = event.getObject();
							var container = new NodeRunner(server, MDCConstants.PROPLOADERCONFIGFOLDER,
									containerprocesses, hdfs, containeridthreads, containeridports,
									object, event);
							Future<Boolean> containerallocated = escontainer.submit(container);
							log.info("Containers Allocated: " + containerallocated.get());
						} catch (InterruptedException e) {
							log.warn("Interrupted!", e);
							// Restore interrupted state...
							Thread.currentThread().interrupt();
						} catch (Exception e) {
							log.error(MDCConstants.EMPTY, e);
						}
					}
				});
			containerlauncher.add(server);
		}
	}

	@AfterClass
	public static void shutdownNodes() throws Exception {
		containers.keySet().stream().flatMap(key -> containers.get(key).stream()).forEach(servers -> {
			try {
				servers.close();
			} catch (IOException e) {
			}
		});
		tes.keySet().stream().flatMap(key -> tes.get(key).stream()).forEach(thr -> thr.stop());
		if (!Objects.isNull(containerlauncher)) {
			containerlauncher.stream().forEach(ss -> {
				ss.close();
			});
		}
		if (!Objects.isNull(es)) {
			es.shutdown();
		}
		if (!Objects.isNull(escontainer)) {
			escontainer.shutdown();
		}
	}


	@Test
	public void testGetContainersBalanced() throws Exception {
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.supplier = () -> 2;
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
		assertEquals(2, bls.size());
		assertEquals(4270834, fbp.totallength);
		fbp.job = new Job();
		fbp.job.jm = new JobMetrics();
		fbp.getDnXref(bls, false);
		fbp.allocateContainersLoadBalanced(bls);
		assertEquals("127.0.0.1_10101", bls.get(0).getExecutorhp());
		assertEquals("127.0.0.1_10101", bls.get(1).getExecutorhp());
	}

	@Test
	public void testGetContainersBalancedMultipleContainer() throws Exception {
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.supplier = () -> 2;
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
		assertEquals(2, bls.size());
		assertEquals(4270834, fbp.totallength);
		fbp.getDnXref(bls, false);
		fbp.allocateContainersLoadBalanced(bls);
		assertEquals("127.0.0.1_10101", bls.get(0).getExecutorhp());
		assertEquals("127.0.0.1_10102", bls.get(1).getExecutorhp());
	}

	@Test
	public void testGetHDFSParitions() throws Exception {
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.supplier = () -> 2;
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
		assertEquals(2, bls.size());
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
		resource.setFreememory(12 * 1024 * 1024 * 1024l);
		resource.setNumberofprocessors(4);
		noderesourcesmap.put("127.0.0.1_20000", resource);
		resource = new Resources();
		resource.setFreememory(6 * 1024 * 1024 * 1024l);
		resource.setNumberofprocessors(4);
		noderesourcesmap.put("127.0.0.1_20001", resource);
		MDCNodesResources.put(noderesourcesmap);
		Map<String, Long> nodestotalblockmem = new ConcurrentHashMap<>();
		fbp.getDnXref(bls, false);
		fbp.getNodesResourcesSorted(bls, nodestotalblockmem);
		assertEquals(2, fbp.nodessorted.size());
		assertEquals("127.0.0.1_20001", fbp.nodessorted.get(0));
		assertEquals("127.0.0.1_20000", fbp.nodessorted.get(1));
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
		resource.setFreememory(12 * 1024 * 1024 * 1024l);
		resource.setNumberofprocessors(4);
		noderesourcesmap.put("127.0.0.1_20000", resource);
		MDCNodesResources.put(noderesourcesmap);
		fbp.pipelineconfig = new PipelineConfig();
		fbp.pipelineconfig.setMaxmem("4096");
		fbp.pipelineconfig.setNumberofcontainers("5");
		fbp.job = new Job();fbp.job.jm = new JobMetrics();
		fbp.isignite = false;
		fbp.getDnXref(bls, false);
		fbp.allocateContainersByResources(bls);
		assertEquals(1, fbp.job.containers.size());
		assertEquals(1, fbp.job.nodes.size());
		fbp.destroyContainers();
		GlobalContainerAllocDealloc.getHportcrs().clear();
	}

	@Test
	public void testGetTaskExecutorsLessResourcesInputCpu() throws Exception {
		ConcurrentMap<String, Resources> noderesourcesmap = new ConcurrentHashMap<>();
		Resources resource = new Resources();
		resource.setFreememory(12 * 1024 * 1024 * 1024l);
		resource.setNumberofprocessors(1);
		noderesourcesmap.put("127.0.0.1_20000", resource);
		MDCNodesResources.put(noderesourcesmap);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.pipelineconfig = new PipelineConfig();
		fbp.pipelineconfig.setMaxmem("4096");
		fbp.pipelineconfig.setNumberofcontainers("5");
		fbp.job = new Job();fbp.job.jm = new JobMetrics();
		fbp.isignite = false;
		FileSystem hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
		fbp.hdfs = hdfs;
		FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airlinesample));
		Path[] paths = FileUtil.stat2Paths(fileStatus);
		fbp.filepaths = Arrays.asList(paths);
		fbp.isblocksuserdefined = false;
		fbp.hdfs = hdfs;
		List<BlocksLocation> bls = fbp.getBlocks(fbp.isblocksuserdefined, 128);
		fbp.getDnXref(bls, false);
		fbp.allocateContainersByResources(bls);
		assertEquals(1, fbp.job.containers.size());
		assertEquals(1, fbp.job.nodes.size());
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
			fbp.job = new Job();fbp.job.jm = new JobMetrics();
			FileSystem hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
			fbp.hdfs = hdfs;
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airlinesample));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			fbp.isblocksuserdefined = false;
			fbp.hdfs = hdfs;
			fbp.filepaths = Arrays.asList(paths);
			List<BlocksLocation> bls = fbp.getBlocks(fbp.isblocksuserdefined, 128);
			fbp.getDnXref(bls, false);
			fbp.allocateContainersByResources(bls);
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
			resource.setFreememory(400 * 1024 * 1024l + 128 * MDCConstants.MB * 3 * Integer.parseInt(MDCProperties
					.get().getProperty(MDCConstants.BYTEBUFFERPOOL_MAX, MDCConstants.BYTEBUFFERPOOL_MAX_DEFAULT)));
			resource.setNumberofprocessors(4);
			noderesourcesmap.put("127.0.0.1_20000", resource);
			MDCNodesResources.put(noderesourcesmap);
			fbp.pipelineconfig = new PipelineConfig();
			fbp.pipelineconfig.setMaxmem("4096");
			fbp.pipelineconfig.setNumberofcontainers("5");
			fbp.isignite = false;
			fbp.job = new Job();
			fbp.job.jm = new JobMetrics();
			FileSystem hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
			fbp.hdfs = hdfs;
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airline1989));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			fbp.isblocksuserdefined = false;
			fbp.hdfs = hdfs;
			fbp.filepaths = Arrays.asList(paths);
			List<BlocksLocation> bls = fbp.getBlocks(fbp.isblocksuserdefined, 128);
			fbp.getDnXref(bls, false);
			fbp.allocateContainersByResources(bls);
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
	public void testGetTaskExecutorsProperInput() throws Exception {
		ConcurrentMap<String, Resources> noderesourcesmap = new ConcurrentHashMap<>();
		Resources resource = new Resources();
		resource.setFreememory(12 * 1024 * 1024 * 1024l);
		resource.setNumberofprocessors(4);
		noderesourcesmap.put("127.0.0.1_20000", resource);
		MDCNodesResources.put(noderesourcesmap);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.pipelineconfig = new PipelineConfig();
		fbp.pipelineconfig.setMaxmem("4096");
		fbp.pipelineconfig.setNumberofcontainers("5");
		fbp.pipelineconfig.setBlocksize("128");
		fbp.job = new Job();fbp.job.jm = new JobMetrics();
		fbp.isignite = false;
		FileSystem hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
		fbp.hdfs = hdfs;
		FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airlinesample));
		Path[] paths = FileUtil.stat2Paths(fileStatus);
		fbp.hdfs = hdfs;
		fbp.isblocksuserdefined = true;
		fbp.filepaths = Arrays.asList(paths);
		List<BlocksLocation> bls = fbp.getBlocks(fbp.isblocksuserdefined, 128 * MDCConstants.MB);
		fbp.getDnXref(bls, false);
		fbp.allocateContainersByResources(bls);
		assertEquals(1, fbp.job.containers.size());
		assertNotNull(fbp.job.containers.get(0));
		assertEquals("127.0.0.1_20000", fbp.job.nodes.iterator().next());
		assertEquals(1, fbp.job.nodes.size());
		fbp.destroyContainers();
		GlobalContainerAllocDealloc.getHportcrs().clear();
	}
}
