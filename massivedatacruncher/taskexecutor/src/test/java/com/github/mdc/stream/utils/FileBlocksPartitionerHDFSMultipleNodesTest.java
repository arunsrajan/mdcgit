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

import java.io.IOException;
import java.net.ServerSocket;
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
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Resources;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.StreamPipelineBase;
import com.github.mdc.tasks.executor.NodeRunner;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FileBlocksPartitionerHDFSMultipleNodesTest extends StreamPipelineBase {
	private static final int NOOFNODES = 5;
	static int teport = 12121;
	static ExecutorService escontainer;
	static ConcurrentMap<String, List<ServerSocket>> containers;
	static ConcurrentMap<String, List<Thread>> tes;
	static List<ServerEndpoint> containerlauncher = new ArrayList<>();
	static Logger log = Logger.getLogger(FileBlocksPartitionerHDFSMultipleNodesTest.class);
	static int nodeindex;
	static FileSystem hdfs;
	static Path[] paths;
	static List<BlocksLocation> bls;
	static ServerEndpoint server = null;

	@BeforeClass
	public static void launchNodes() throws Exception {
		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, "mdctest.properties");
		hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
		FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airlines));
		paths = FileUtil.stat2Paths(fileStatus);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.hdfs = hdfs;
		fbp.filepaths = Arrays.asList(paths);
		bls = fbp.getBlocks(true, 128 * MDCConstants.MB);
		containers = new ConcurrentHashMap<>();
		tes = new ConcurrentHashMap<>();
		escontainer = Executors.newFixedThreadPool(100);
		var containerprocesses = new ConcurrentHashMap<String, Map<String, Process>>();
		var containeridthreads = new ConcurrentHashMap<String, Map<String, List<Thread>>>();
		var containeridports = new ConcurrentHashMap<String, List<Integer>>();
		ConcurrentMap<String, Resources> noderesourcesmap = new ConcurrentHashMap<>();
		MDCNodesResources.put(noderesourcesmap);
		for (; nodeindex < NOOFNODES; nodeindex++) {
			Resources resource = new Resources();
			int memory = 64;
			resource.setFreememory(memory * 1024 * 1024 * 1024l);
			resource.setNumberofprocessors(4);
			noderesourcesmap.put("127.0.0.1_" + (20000 + nodeindex), resource);
			server = Utils.getServerKryoNetty(20000+nodeindex,
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
		if (!Objects.isNull(escontainer)) {
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
		Map<String, Long> nodestotalblockmem = new ConcurrentHashMap<>();
		fbp.getDnXref(bls, false);
		fbp.getNodesResourcesSorted(bls, nodestotalblockmem);
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
		fbp.job.jm = new JobMetrics();
		fbp.isignite = false;
		fbp.getDnXref(bls, false);
		fbp.allocateContainersByResources(bls);
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
		fbp.job.jm = new JobMetrics();
		fbp.isignite = false;
		fbp.filepaths = Arrays.asList(paths);
		fbp.isblocksuserdefined = false;
		fbp.hdfs = hdfs;
		fbp.getDnXref(bls, false);
		fbp.allocateContainersByResources(bls);
		log.info(fbp.job.nodes);
		log.info(fbp.job.containers);
		fbp.destroyContainers();
		GlobalContainerAllocDealloc.getHportcrs().clear();
		log.info("FileBlocksPartitionerHDFSMultipleNodesTest.testGetTaskExecutorsProperInput() Exiting------------------------------");
	}
}
