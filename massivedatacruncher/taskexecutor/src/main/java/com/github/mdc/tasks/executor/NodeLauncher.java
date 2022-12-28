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

import java.net.URI;
import java.net.URL;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.mdc.common.HeartBeat;
import com.github.mdc.common.HeartBeatStream;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.ServerUtils;
import com.github.mdc.common.StreamDataCruncher;
import com.github.mdc.common.Utils;
import com.github.mdc.common.WebResourcesServlet;
import com.github.mdc.tasks.executor.web.NodeWebServlet;
import com.github.mdc.tasks.executor.web.ResourcesMetricsServlet;

public class NodeLauncher {
	static Logger log = LoggerFactory.getLogger(NodeLauncher.class);
	static Registry server = null;
	static StreamDataCruncher stub = null;
	static StreamDataCruncher sdc =	null;
	public static void main(String[] args) throws Exception {
		org.burningwave.core.assembler.StaticComponentContainer.Modules.exportAllToAll();
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, MDCConstants.MDC_PROPERTIES);
		var port = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.NODE_PORT));
		try (var hbss = new HeartBeatStream();) {
			var pingdelay = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PINGDELAY));
			var host = NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST));
			hbss.init(0, port, host, 0, pingdelay, "");
			var hb = new HeartBeat();
			hb.init(0, port, host, 0, pingdelay, "");
			hbss.ping();
			hb.ping();
			var teport = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PORT));
			var escontainer = Executors.newFixedThreadPool(1);

			var hdfs = FileSystem.get(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL)), new Configuration());
			var containerprocesses = new ConcurrentHashMap<String, Map<String, Process>>();
			var containeridthreads = new ConcurrentHashMap<String, Map<String, List<Thread>>>();
			var containeridports = new ConcurrentHashMap<String, List<Integer>>();
			var su = new ServerUtils();
			su.init(port + 50,
					new NodeWebServlet(containerprocesses), MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX,
					new WebResourcesServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.RESOURCES + MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX,
					new ResourcesMetricsServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.DATA + MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX
			);
			su.start();
			server = LocateRegistry.createRegistry(port);
			sdc = new StreamDataCruncher() {
	            public Object postObject(Object object) {
					try {
						var container = new NodeRunner(MDCConstants.PROPLOADERCONFIGFOLDER,
								containerprocesses, hdfs, containeridthreads, containeridports,
								object);
						Future<Object> containerallocated = escontainer.submit(container);
						Object obj = containerallocated.get();
						log.info("Chamber reply: " + obj);
						return obj;
					} catch (InterruptedException e) {
						log.warn("Interrupted!", e);
						// Restore interrupted state...
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						log.error(MDCConstants.EMPTY, e);
					}
					return null;
				}
			};
			stub = (StreamDataCruncher) UnicastRemoteObject.exportObject(sdc, 0);
			server.rebind(MDCConstants.BINDTESTUB, stub);
			log.info("NodeLauncher kickoff at port {}.....",
					MDCProperties.get().getProperty(MDCConstants.NODE_PORT));
			log.info("Reckoning closedown lock...");
			var cdl = new CountDownLatch(1);
			Utils.addShutdownHook(() -> {
				try {
					containerprocesses.keySet().stream().map(containerprocesses::get)
							.flatMap(mapproc -> mapproc.keySet().stream().map(key -> mapproc.get(key)).collect(Collectors.toList()).stream()).forEach(proc -> {
						log.debug("Destroying the Container Process: " + proc);
						proc.destroy();
					});
					log.debug("Stopping and closes all the connections...");
					log.debug("Destroying...");
					hdfs.close();
					cdl.countDown();
					Runtime.getRuntime().halt(0);
				} catch (Exception e) {
					log.debug("", e);
				}
			});
			cdl.await();
		}
		catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		}
		catch (Exception ex) {
			log.error("Unable to start Node Manager due to ", ex);
		}
	}

}
