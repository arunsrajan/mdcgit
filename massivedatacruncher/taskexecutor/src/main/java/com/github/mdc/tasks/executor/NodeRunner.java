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

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.esotericsoftware.kryonetty.ServerEndpoint;
import com.esotericsoftware.kryonetty.network.ReceiveEvent;
import com.github.mdc.common.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.LoggerFactory;

import static java.util.Objects.nonNull;

public class NodeRunner implements Callable<Boolean> {
	private static org.slf4j.Logger log = LoggerFactory.getLogger(NodeRunner.class);
	ServerEndpoint server;
	String proploaderpath;
	ConcurrentMap<String, Map<String, Process>> containerprocesses;
	FileSystem hdfs;
	ConcurrentMap<String, Map<String, List<Thread>>> containeridcontainerthreads;
	ConcurrentMap<String, List<Integer>> containeridports;
	Object receivedobject;
	ReceiveEvent event;
	public NodeRunner(ServerEndpoint server, String proploaderpath,
			ConcurrentMap<String, Map<String, Process>> containerprocesses, FileSystem hdfs,
			ConcurrentMap<String, Map<String, List<Thread>>> containeridcontainerthreads,
			ConcurrentMap<String, List<Integer>> containeridports,
			Object receivedobject,
			ReceiveEvent event) {
		this.server = server;
		this.proploaderpath = proploaderpath;
		this.containerprocesses = containerprocesses;
		this.hdfs = hdfs;
		this.containeridcontainerthreads = containeridcontainerthreads;
		this.containeridports = containeridports;
		this.receivedobject= receivedobject;
		this.event = event;
	}

	ClassLoader cl;

	public Boolean call() {
		try {
			Object deserobj = receivedobject;
			if (deserobj instanceof AllocateContainers ac) {
				List<Integer> ports = new ArrayList<>();
				for (int numport = 0; numport < ac.getNumberofcontainers(); numport++) {
					try(ServerSocket s = new ServerSocket(0);){
						int port = s.getLocalPort();
						log.info("Allocating Port " + port);
						ports.add(port);
					}
				}
				containeridports.put(ac.getContainerid(), ports);
				server.send(event.getCtx(), ports);
			} else if (deserobj instanceof LaunchContainers lc) {
				Map<String, Process> processes = new ConcurrentHashMap<>();
				Map<String, List<Thread>> threads = new ConcurrentHashMap<>();
				List<Integer> ports = new ArrayList<>();
				Process proc;
				for (int port = 0; port < lc.getCla().getNumberofcontainers(); port++) {
					var cr = lc.getCla().getCr().get(port);
					log.info("Launching Container {}", (cr.getPort()));
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
				server.send(event.getCtx(), ports);
			} else if (deserobj instanceof DestroyContainers dc) {
				log.debug("Destroying the Containers with id: " + dc.getContainerid());
				Map<String, Process> processes = containerprocesses.remove(dc.getContainerid());
				if (!Objects.isNull(processes)) {
					processes.entrySet().stream().forEach(entry -> {
						log.info("In DCs Destroying the Container Process: " + entry);
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
						log.info("Destroying the Container Process: " + proc);
						destroyProcess(taskexecutorport, proc);
					});
					processes.remove(taskexecutorport);
				} else {
					containerprocesses.keySet().stream().forEach(key -> {
						containerprocesses.get(key).keySet().stream().filter(port -> port.equals(dc.getContainerhp().split(MDCConstants.UNDERSCORE)[1]))
								.forEach(port -> {
									Process proc = containerprocesses.get(key).get(port);
									if (nonNull(proc)) {
										log.info("Destroying the Container Process: " + proc);
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
				server.send(event.getCtx(), numberofbytesskipped);
			}
			return true;
		} catch (Exception ex) {
			log.error("Task completed in error", ex);
		}
		return false;
	}
	
	public void destroyProcess(String port, Process proc) {
		try {
			TaskExecutorShutdown taskExecutorshutdown = new TaskExecutorShutdown();
			log.info("Initiated destroying the TaskExecutor process {}",MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)+MDCConstants.UNDERSCORE+port);
			Utils.writeObject(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)+MDCConstants.UNDERSCORE+port, taskExecutorshutdown);
			log.info("Checking the Process is Alive for {} ",MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)+MDCConstants.UNDERSCORE+port);
			while(proc.isAlive()){
				log.info("Attempting destroying the TaskExecutor again {}", MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)+MDCConstants.UNDERSCORE+port);
				Thread.sleep(500);
			}
			log.info("Process {} Destroyed for the port {} ",proc,port);
		}
		catch(Exception ex) {
			log.error("Destroy failed for the process "+proc, ex);
		}
	}

}
