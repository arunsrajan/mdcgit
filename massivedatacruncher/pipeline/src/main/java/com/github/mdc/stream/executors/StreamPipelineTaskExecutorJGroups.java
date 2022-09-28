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
package com.github.mdc.stream.executors;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.ehcache.Cache;
import org.jgroups.JChannel;

import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.RemoteDataFetch;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.Task;
import com.github.mdc.common.Utils;
import com.github.mdc.common.WhoIsResponse;

/**
 * 
 * @author Arun 
 * Task executors thread for standalone task executors daemon.
 */
@SuppressWarnings("rawtypes")
public final class StreamPipelineTaskExecutorJGroups extends StreamPipelineTaskExecutor {
	private static Logger log = Logger.getLogger(StreamPipelineTaskExecutorJGroups.class);
	private List<Task> tasks = null;
	Map<String, JobStage> jsidjsmap;
	public double timetaken = 0.0;
	public JChannel channel;
	private int port;

	public StreamPipelineTaskExecutorJGroups(Map<String, JobStage> jsidjsmap, List<Task> tasks, int port, Cache cache) {
		super(jsidjsmap.get(tasks.get(0).jobid + tasks.get(0).stageid), cache);
		this.jsidjsmap = jsidjsmap;
		this.tasks = tasks;
		this.port = port;
	}

	@Override
	public Boolean call() {
		log.debug("Entered MassiveDataStreamJGroupsTaskExecutor.call");
		var taskstatusmap = tasks.parallelStream()
				.map(task -> task.taskid)
				.collect(Collectors.toMap(key -> key, value -> WhoIsResponse.STATUS.YETTOSTART));
		var taskstatusconcmapreq = new ConcurrentHashMap<>(
				taskstatusmap);
		var taskstatusconcmapresp = new ConcurrentHashMap<String, WhoIsResponse.STATUS>();
		var hdfsfilepath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL);
		String host = NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST));
		try (var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());) {
			this.hdfs = hdfs;
			channel = Utils.getChannelTaskExecutor(jobstage.getJobid(),
					host,
					port, taskstatusconcmapreq, taskstatusconcmapresp);
			log.info("Tasks In Jgroups executor: " + tasks + " in host: " + host + " port: " + port);
			for (var task : tasks) {
				log.info("Task In Jgroups executor: " + task + " in host: " + host + " port: " + port + " with task hostport: " + task.hostport);
				this.task = task;
				this.jobstage = jsidjsmap.get(task.jobid + task.stageid);
				var stageTasks = getStagesTask();
				var stagePartition = task.taskid;
				try {
					var taskspredecessor = task.taskspredecessor;
					if (!taskspredecessor.isEmpty()) {
						var taskids = taskspredecessor.parallelStream()
								.map(tk -> tk.taskid)
								.collect(Collectors.toList());
						var breakloop = false;
						while (true) {
							var tasktatusconcmap = new ConcurrentHashMap<String, WhoIsResponse.STATUS>(
									taskstatusconcmapreq);
							tasktatusconcmap.putAll(taskstatusconcmapresp);
							breakloop = true;
							for (var taskid : taskids) {
								if (taskstatusconcmapresp.get(taskid) != null
										&& taskstatusconcmapresp
										.get(taskid) != WhoIsResponse.STATUS.COMPLETED) {
									Utils.whois(channel, taskid);
									breakloop = false;
									continue;
								} else if (tasktatusconcmap.get(taskid) != null) {
									if (tasktatusconcmap
											.get(taskid) != WhoIsResponse.STATUS.COMPLETED) {
										breakloop = false;
										continue;
									}

								} else {
									Utils.whois(channel, taskid);
									breakloop = false;
									continue;
								}
							}
							if (breakloop)
								break;
							Thread.sleep(1000);
						}
					}
					log.debug("Submitted Stage " + stagePartition);
					log.debug("Running Stage " + stageTasks);

					taskstatusconcmapreq.put(stagePartition, WhoIsResponse.STATUS.RUNNING);
					if (task.input != null && task.parentremotedatafetch != null) {
						var numinputs = task.parentremotedatafetch.length;
						for (var inputindex = 0; inputindex < numinputs; inputindex++) {
							var input = task.parentremotedatafetch[inputindex];
							if (input != null) {
								var rdf = input;
								InputStream is = RemoteDataFetcher.readIntermediatePhaseOutputFromFS(rdf.getJobid(),
										getIntermediateDataRDF(rdf.getTaskid()));
								if (Objects.isNull(is)) {
									RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
									task.input[inputindex] =
											new ByteArrayInputStream(rdf.getData());
								} else {
									task.input[inputindex] = is;
								}
							}
						}
					}

					var timetakenseconds = computeTasks(task, hdfs);
					log.debug("Completed Stage " + stagePartition + " in " + timetakenseconds);
					taskstatusconcmapreq.put(stagePartition, WhoIsResponse.STATUS.COMPLETED);
				} finally {

				}
			}
			log.debug("StagePartitionId with Stage Statuses: " + taskstatusconcmapreq
					+ " WhoIs Response stauses: " + taskstatusconcmapresp);
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.error("Failed Stage " + tasks, ex);
		}
		log.debug("Exiting MassiveDataStreamJGroupsTaskExecutor.call");
		return completed;
	}

}
