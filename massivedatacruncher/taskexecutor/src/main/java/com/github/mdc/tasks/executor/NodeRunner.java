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
package com.github.mdc.tasks.executor;

import static java.util.Objects.nonNull;

import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.LoggerFactory;

import com.github.mdc.common.AllocateContainers;
import com.github.mdc.common.ContainerLauncher;
import com.github.mdc.common.DestroyContainer;
import com.github.mdc.common.DestroyContainers;
import com.github.mdc.common.HDFSBlockUtils;
import com.github.mdc.common.LaunchContainers;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.SkipToNewLine;
import com.github.mdc.common.TaskExecutorShutdown;
import com.github.mdc.common.Utils;

public class NodeRunner implements Callable<Object> {
	private static org.slf4j.Logger log = LoggerFactory.getLogger(NodeRunner.class);
	String proploaderpath;
	ConcurrentMap<String, Map<String, Process>> containerprocesses;
	FileSystem hdfs;
	ConcurrentMap<String, Map<String, List<Thread>>> containeridcontainerthreads;
	ConcurrentMap<String, List<Integer>> containeridports;
	Object receivedobject;
	public NodeRunner(String proploaderpath,
			ConcurrentMap<String, Map<String, Process>> containerprocesses, FileSystem hdfs,
			ConcurrentMap<String, Map<String, List<Thread>>> containeridcontainerthreads,
			ConcurrentMap<String, List<Integer>> containeridports,
			Object receivedobject) {
		this.proploaderpath = proploaderpath;
		this.containerprocesses = containerprocesses;
		this.hdfs = hdfs;
		this.containeridcontainerthreads = containeridcontainerthreads;
		this.containeridports = containeridports;
		this.receivedobject= receivedobject;
	}

	ClassLoader cl;

	public Object call() {
		try {
			Object deserobj = receivedobject;
			if (deserobj instanceof AllocateContainers ac) {
				List<Integer> ports = new ArrayList<>();
				for (int numport = 0; numport < ac.getNumberofcontainers(); numport++) {
					try(ServerSocket s = new ServerSocket(0);){
						int port = s.getLocalPort();
						log.info("Alloting Port " + port);
						ports.add(port);
					}
				}
				containeridports.put(ac.getContainerid(), ports);
				return ports;
			} else if (deserobj instanceof LaunchContainers lc) {
				Map<String, Process> processes = new ConcurrentHashMap<>();
				Map<String, List<Thread>> threads = new ConcurrentHashMap<>();
				List<Integer> ports = new ArrayList<>();
				Process proc;
				for (int port = 0; port < lc.getCla().getNumberofcontainers(); port++) {
					var cr = lc.getCla().getCr().get(port);
					log.info("Dispatching chamber {}....", (cr.getPort()));
					proc = processes.get((cr.getPort()) + MDCConstants.EMPTY);
					if (Objects.isNull(proc)) {
						proc = ContainerLauncher.spawnMDCContainer((cr.getPort()) + MDCConstants.EMPTY,
								(String) MDCProperties.get().get(MDCConstants.CACHEDISKPATH),
								TaskExecutorRunner.class, proploaderpath, cr);
						processes.put((cr.getPort()) + MDCConstants.EMPTY, proc);
					}
					ports.add(cr.getPort());
				}
				processes.keySet().parallelStream()
						.map(prockey -> (Tuple2<String, Process>) Tuple.tuple(prockey, processes.get(prockey)))
						.forEach((Tuple2<String, Process> tuple) -> {
							Thread thr = new Thread() {
								public void run() {
									try {
										log.debug("Printing Container Logs");
										InputStream istr = tuple.v2.getInputStream();
										while (true) {
											log.debug("{}",IOUtils.readLines(istr, StandardCharsets.UTF_8));
											Thread.sleep(5000);
										}
									} catch (InterruptedException e) {
										log.warn("Interrupted!", e);
										// Restore interrupted state...
										Thread.currentThread().interrupt();
									} catch (Exception ex) {
										log.error("Unable to Launch Container:", ex);
									}
								}
							};
							thr.start();
							threads.put(tuple.v1, new ArrayList<>());
							threads.get(tuple.v1).add(thr);
							thr = new Thread() {
								public void run() {
									try {
										log.debug("Printing Container Error Logs");
										InputStream istr = tuple.v2.getErrorStream();
										while (true) {
											log.debug("{}",IOUtils.readLines(istr, StandardCharsets.UTF_8));
											Thread.sleep(5000);
										}
									} catch (InterruptedException e) {
										log.warn("Interrupted!", e);
										// Restore interrupted state...
										Thread.currentThread().interrupt();
									} catch (Exception ex) {
										log.error("Unable to Launch Container:", ex);
									}
								}
							};
							thr.start();
							threads.get(tuple.v1).add(thr);
						});
				containeridcontainerthreads.put(lc.getContainerid(), threads);
				containerprocesses.put(lc.getContainerid(), processes);
				return ports;
			} else if (deserobj instanceof DestroyContainers dc) {
				log.debug("Destroying the Containers with id: " + dc.getContainerid());
				Map<String, Process> processes = containerprocesses.remove(dc.getContainerid());
				if (!Objects.isNull(processes)) {
					processes.entrySet().stream().forEach(entry -> {
						log.info("Eradicate the chamber case: " + entry);
						destroyProcess(entry.getKey(),entry.getValue());
					});
				}
				Map<String, List<Thread>> threads = containeridcontainerthreads.remove(dc.getContainerid());
				if (!Objects.isNull(threads)) {
					threads.keySet().stream().map(threads::get).flatMap(thrlist -> thrlist.stream())
							.forEach(thr -> thr.stop());
				}
			} else if (deserobj instanceof DestroyContainer dc) {
				log.debug("Destroying the Container with id: " + dc.getContainerid());
				Map<String, Process> processes = containerprocesses.get(dc.getContainerid());
				if (!Objects.isNull(processes)) {
					String taskexecutorport = dc.getContainerhp().split(MDCConstants.UNDERSCORE)[1];
					processes.keySet().stream()
							.filter(key -> key.equals(taskexecutorport))
							.map(key -> processes.get(key)).forEach(proc -> {
						log.info("Eradicate the chamber case: " + proc);
						destroyProcess(taskexecutorport, proc);
					});
					processes.remove(taskexecutorport);
				} else {
					containerprocesses.keySet().stream().forEach(key -> {
						containerprocesses.get(key).keySet().stream().filter(port -> port.equals(dc.getContainerhp().split(MDCConstants.UNDERSCORE)[1]))
								.forEach(port -> {
									Process proc = containerprocesses.get(key).get(port);
									if (nonNull(proc)) {
										log.info("Eradicate the chamber case: " + proc);
										destroyProcess(port, proc);
									}
								});
					});
				}
				Map<String, List<Thread>> threads = containeridcontainerthreads.get(dc.getContainerid());
				if (!Objects.isNull(threads)) {
					threads.keySet().stream()
							.filter(key -> key.equals(dc.getContainerhp().split(MDCConstants.UNDERSCORE)[1]))
							.map(threads::get).flatMap(thrlist -> thrlist.stream()).forEach(thr -> thr.stop());
					threads.remove(dc.getContainerhp().split(MDCConstants.UNDERSCORE)[1]);
				}
			} else if (deserobj instanceof SkipToNewLine stnl) {
				long numberofbytesskipped = HDFSBlockUtils.skipBlockToNewLine(hdfs, stnl.lblock, stnl.l,
						stnl.xrefaddress);
				return numberofbytesskipped;
			}
			return true;
		} catch (Exception ex) {
			log.error("Incomplete task with error", ex);
		}
		return false;
	}
	
	public void destroyProcess(String port, Process proc) {
		try {
			TaskExecutorShutdown taskExecutorshutdown = new TaskExecutorShutdown();
			log.info("Initiated eradicating the chamber case: {}",MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)+MDCConstants.UNDERSCORE+port);
			Utils.getResultObjectByInput(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)+MDCConstants.UNDERSCORE+port, taskExecutorshutdown);
			log.info("Intercepting the chamber case conscious for {} ",MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)+MDCConstants.UNDERSCORE+port);
			while(proc.isAlive()){
				log.info("Seeking the chamber case stats {}", MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)+MDCConstants.UNDERSCORE+port);
				Thread.sleep(500);
			}
			log.info("The chamber case {} shattered for the port {} ",proc,port);
		}
		catch(Exception ex) {
			log.error("Destroy failed for the process "+proc, ex);
		}
	}

}
