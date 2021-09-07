package com.github.mdc.tasks.executor;

import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.github.mdc.common.AllocateContainers;
import com.github.mdc.common.ContainerLauncher;
import com.github.mdc.common.DestroyContainer;
import com.github.mdc.common.DestroyContainers;
import com.github.mdc.common.HDFSBlockUtils;
import com.github.mdc.common.LaunchContainers;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCExecutorThreadFactory;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.SkipToNewLine;
import com.github.mdc.common.Utils;

public class Container implements Callable<Boolean> {
	private static Logger log = Logger.getLogger(Container.class);
	ExecutorService es = Executors.newWorkStealingPool();
	Socket sock;
	AtomicInteger portinc;
	String proploaderpath;
	ConcurrentMap<String, Map<String,Process>> containerprocesses;
	FileSystem hdfs;
	ConcurrentMap<String, Map<String,List<Thread>>> containeridcontainerthreads;
	ConcurrentMap<String, List<Integer>> containeridports;

	public Container(Socket sock, AtomicInteger portinc, String proploaderpath,
			ConcurrentMap<String, Map<String,Process>> containerprocesses, FileSystem hdfs,
			ConcurrentMap<String, Map<String,List<Thread>>> containeridcontainerthreads,
			ConcurrentMap<String, List<Integer>> containeridports) {
		this.sock = sock;
		this.portinc = portinc;
		this.proploaderpath = proploaderpath;
		this.containerprocesses = containerprocesses;
		this.hdfs = hdfs;
		this.containeridcontainerthreads = containeridcontainerthreads;
		this.containeridports = containeridports;
	}

	ClassLoader cl;

	public Boolean call() {
		try {
			Object deserobj = Utils.readObject(sock);
			if (deserobj instanceof AllocateContainers ac) {
				List<Integer> ports = new ArrayList<>();
				for (int port = 0; port < ac.getNumberofcontainers(); port++) {
					log.info("Allocating Port " + (port + portinc.get()));
					ports.add(port + portinc.get());
				}
				containeridports.put(ac.getContainerid(), ports);
				portinc.set(portinc.get() + ac.getNumberofcontainers());
				Utils.writeObjectByStream(sock.getOutputStream(), ports);
			} else if (deserobj instanceof LaunchContainers lc) {
				Map<String, Process> processes = new ConcurrentHashMap<>();
				Map<String, List<Thread>> threads = new ConcurrentHashMap<>();
				List<Integer> ports = new ArrayList<>();
				Process proc;
				for (int port = 0; port < lc.getCla().getNumberofcontainers(); port++) {
					var cr = lc.getCla().getCr().get(port);
					log.info("Launching Container " + (cr.getPort()));
					proc = processes.get((cr.getPort()) + MDCConstants.EMPTY);
					if(Objects.isNull(proc)) {
						proc = ContainerLauncher.spawnMDCContainer((cr.getPort()) + MDCConstants.EMPTY,
								(String) MDCProperties.get().get(MDCConstants.CACHEDISKPATH),
								MassiveDataTaskExecutorDeamon.class, proploaderpath, cr);
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
											log.debug(IOUtils.readLines(istr, StandardCharsets.UTF_8));
											Thread.sleep(5000);
										}
									} catch (Exception ex) {
										log.debug("Unable to Launch Container:", ex);
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
											log.debug(IOUtils.readLines(istr, StandardCharsets.UTF_8));
											Thread.sleep(5000);
										}
									} catch (Exception ex) {
										log.debug("Unable to Launch Container:", ex);
									}
								}
							};
							thr.start();
							threads.get(tuple.v1).add(thr);
						});
				containeridcontainerthreads.put(lc.getContainerid(), threads);
				containerprocesses.put(lc.getContainerid(), processes);
				Utils.writeObjectByStream(sock.getOutputStream(), ports);
			} else if (deserobj instanceof DestroyContainers dc) {
				log.debug("Destroying the Containers with id: " + dc.getContainerid());
				Map<String, Process> processes = containerprocesses.remove(dc.getContainerid());
				log.debug("Destroying the Container Processes: " + processes);
				if (!Objects.isNull(processes)) {
					processes.keySet().stream().map(key -> processes.get(key)).forEach(proc -> {
						log.debug("Destroying the Container Process: " + proc);
						proc.destroy();
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
				log.debug("Destroying the Container Processes: " + processes);
				if (!Objects.isNull(processes)) {
					processes.keySet().stream()
							.filter(key -> key.equals(dc.getContainerhp().split(MDCConstants.UNDERSCORE)[1]))
							.map(key -> processes.get(key)).forEach(proc -> {
								log.debug("Destroying the Container Process: " + proc);
								proc.destroy();
							});
					processes.remove(dc.getContainerhp().split(MDCConstants.UNDERSCORE)[1]);
				} else {
					containerprocesses.keySet().stream().forEach(key -> {
						containerprocesses.get(key).keySet().stream().filter(port -> port.equals(dc.getContainerhp().split(MDCConstants.UNDERSCORE)[1]))
						.map(port -> containerprocesses.get(key).get(port))
						.filter(proc->!Objects.isNull(proc)).forEach(proc -> {
							log.debug("Destroying the Container Process: " + proc);
							proc.destroy();
						});;
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
				Utils.writeObjectByStream(sock.getOutputStream(), numberofbytesskipped);
			}
			return true;
		} catch (Exception ex) {
			log.info("MRJob Execution Problem", ex);
		}
		return false;
	}

}
