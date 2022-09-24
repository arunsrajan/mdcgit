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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.ehcache.Cache;
import org.h2.util.IOUtils;

import com.esotericsoftware.kryonetty.ServerEndpoint;
import com.esotericsoftware.kryonetty.network.ReceiveEvent;
import com.github.mdc.common.ApplicationTask.TaskStatus;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.ByteBufferInputStream;
import com.github.mdc.common.ByteBufferOutputStream;
import com.github.mdc.common.CloseStagesGraphExecutor;
import com.github.mdc.common.Context;
import com.github.mdc.common.FileSystemSupport;
import com.github.mdc.common.FreeResourcesCompletedJob;
import com.github.mdc.common.HdfsBlockReader;
import com.github.mdc.common.HeartBeatStream;
import com.github.mdc.common.HeartBeatTaskScheduler;
import com.github.mdc.common.HeartBeatTaskSchedulerStream;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCConstants.STORAGE;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.ReducerValues;
import com.github.mdc.common.RemoteDataFetch;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.RetrieveData;
import com.github.mdc.common.RetrieveKeys;
import com.github.mdc.common.Task;
import com.github.mdc.common.TasksGraphExecutor;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutor;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorInMemory;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorInMemoryDisk;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorJGroups;

public class TaskExecutor implements Runnable {
	private static Logger log = Logger.getLogger(TaskExecutor.class);
	ServerEndpoint s;
	int port;
	ExecutorService es;
	ConcurrentMap<String, OutputStream> resultstream;
	Configuration configuration;
	Map<String, Object> apptaskexecutormap;
	Cache inmemorycache;
	Object deserobj;
	Map<String, Object> jobstageexecutormap;
	Map<String, Map<String, Object>> jobidstageidexecutormap;
	Map<String, HeartBeatTaskScheduler> hbtsappid = new ConcurrentHashMap<>();
	Map<String, HeartBeatTaskSchedulerStream> hbtssjobid = new ConcurrentHashMap<>();
	Map<String, HeartBeatStream> containeridhbss = new ConcurrentHashMap<>();
	Map<String, JobStage> jobidstageidjobstagemap;
	Queue<Object> taskqueue;
	ReceiveEvent event;
	@SuppressWarnings({"rawtypes"})
	public TaskExecutor(ServerEndpoint s, ClassLoader cl, int port, ExecutorService es, Configuration configuration,
			Map<String, Object> apptaskexecutormap, Map<String, Object> jobstageexecutormap,
			ConcurrentMap<String, OutputStream> resultstream, Cache inmemorycache, Object deserobj,
			Map<String, HeartBeatTaskScheduler> hbtsappid, Map<String, HeartBeatTaskSchedulerStream> hbtssjobid,
			Map<String, HeartBeatStream> containeridhbss,
			Map<String, Map<String, Object>> jobidstageidexecutormap, Queue<Object> taskqueue,
			Map<String, JobStage> jobidstageidjobstagemap, ReceiveEvent event) {
		this.s = s;
		this.cl = cl;
		this.port = port;
		this.es = es;
		this.configuration = configuration;
		this.apptaskexecutormap = apptaskexecutormap;
		this.resultstream = resultstream;
		this.inmemorycache = inmemorycache;
		this.jobstageexecutormap = jobstageexecutormap;
		this.deserobj = deserobj;
		this.hbtsappid = hbtsappid;
		this.hbtssjobid = hbtssjobid;
		this.containeridhbss = containeridhbss;
		this.jobidstageidexecutormap = jobidstageidexecutormap;
		this.taskqueue = taskqueue;
		this.jobidstageidjobstagemap = jobidstageidjobstagemap;
		this.event = event;
	}

	ClassLoader cl;

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void run() {
		log.debug("Started the run------------------------------------------------------");
		try {
			if (deserobj instanceof JobStage jobstage) {
				log.info("Received Job Stage: " + jobstage);
				jobidstageidjobstagemap.put(jobstage.getJobid() + jobstage.getStageid(), jobstage);
			} else if (deserobj instanceof Task task) {
				log.info("Received Task: " + task);
				var taskexecutor = jobstageexecutormap.get(task.jobid + task.stageid + task.taskid);
				var hbtss = hbtssjobid.get(task.jobid);
				var mdste = (StreamPipelineTaskExecutor) taskexecutor;
				if (taskexecutor == null || mdste.isCompleted() && task.taskstatus == Task.TaskStatus.FAILED) {
					var key = task.jobid + task.stageid;
					mdste = task.storage == STORAGE.INMEMORY_DISK ? new StreamPipelineTaskExecutorInMemoryDisk(jobidstageidjobstagemap.get(key), resultstream,
							inmemorycache, hbtss) : task.storage == STORAGE.INMEMORY ? new StreamPipelineTaskExecutorInMemory(jobidstageidjobstagemap.get(key),
							resultstream, inmemorycache) : new StreamPipelineTaskExecutor(jobidstageidjobstagemap.get(key), inmemorycache);
					mdste.setTask(task);
					mdste.setHbtss(hbtss);
					mdste.setExecutor(es);
					jobstageexecutormap.remove(key + task.taskid);
					jobstageexecutormap.put(key + task.taskid, mdste);
					Map<String, Object> stageidexecutormap;
					if (Objects.isNull(jobidstageidexecutormap.get(task.jobid))) {
						stageidexecutormap = new ConcurrentHashMap<>();
						jobidstageidexecutormap.put(task.jobid, stageidexecutormap);
					} else {
						stageidexecutormap = (Map<String, Object>) jobidstageidexecutormap.get(task.jobid);
					}
					stageidexecutormap.put(task.stageid, mdste);
					CompletableFuture.runAsync(mdste, es);
					log.info("Submitted Task for execution: " + deserobj);
				} else if (mdste.isCompleted()) {
					hbtss.setTimetakenseconds(mdste.getHbtss().getTimetakenseconds());
					hbtss.pingOnce(task, Task.TaskStatus.COMPLETED, new Long[]{mdste.starttime, mdste.endtime}, mdste.getHbtss().getTimetakenseconds(), null);
				}
			} else if (deserobj instanceof TasksGraphExecutor stagesgraph) {
				int numoftasks = stagesgraph.getTasks().size();
				var key = stagesgraph.getTasks().get(numoftasks - 1).jobid + stagesgraph.getTasks().get(numoftasks - 1).stageid + stagesgraph.getTasks().get(numoftasks - 1).taskid;
				var taskexecutor = jobstageexecutormap.get(key);
				var mdste = (StreamPipelineTaskExecutorJGroups) taskexecutor;
				if (taskexecutor == null) {
					mdste = new StreamPipelineTaskExecutorJGroups(jobidstageidjobstagemap, stagesgraph.getTasks(),
							port, inmemorycache);
					mdste.setExecutor(es);
					for (Task task :stagesgraph.getTasks()) {
						jobstageexecutormap.put(task.jobid + task.stageid + task.taskid, mdste);
					}
					CompletableFuture.runAsync(mdste, es);
				}
			} else if (deserobj instanceof CloseStagesGraphExecutor closestagesgraph) {
				var key = closestagesgraph.getTasks().get(0).jobid + closestagesgraph.getTasks().get(0).stageid + closestagesgraph.getTasks().get(0).taskid;
				var taskexecutor = jobstageexecutormap.get(key);
				var mdste = (StreamPipelineTaskExecutorJGroups) taskexecutor;
				if (taskexecutor != null) {
					mdste.channel.close();
					for (Task task :closestagesgraph.getTasks()) {
						jobstageexecutormap.remove(task.jobid + task.stageid + task.taskid);
						File todelete = new File(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + MDCConstants.FORWARD_SLASH
								+ FileSystemSupport.MDS + MDCConstants.FORWARD_SLASH + task.jobid + MDCConstants.FORWARD_SLASH + task.taskid);
						todelete.delete();
						log.info("Cleaning Up the Intermedite Task " + todelete);
					}
					File jobtmpdir = new File(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + MDCConstants.FORWARD_SLASH
							+ FileSystemSupport.MDS + MDCConstants.FORWARD_SLASH + closestagesgraph.getTasks().get(0).jobid);
					File[] taskintermediatefiles = jobtmpdir.listFiles();
					if(jobtmpdir.isDirectory()) {
						if(Objects.isNull(taskintermediatefiles) || 
								Objects.nonNull(taskintermediatefiles) 
								&& taskintermediatefiles.length==0) {
							jobtmpdir.delete();
						}
					}
					s.send(event.getCtx(), true);
				}
			} else if (deserobj instanceof FreeResourcesCompletedJob cce) {
				var jsem = jobidstageidexecutormap.remove(cce.getJobid());
				if (!Objects.isNull(jsem)) {
					var keys = jsem.keySet();
					for (var key : keys) {
						jsem.remove(key);
						jobstageexecutormap.remove(cce.getJobid() + key);
						log.info("Removed stages: " + cce.getJobid() + key);
					}
				}
				var hbtss = hbtssjobid.remove(cce.getJobid());
				if (!Objects.isNull(hbtss)) {
					log.info("Hearbeat task scheduler stream closing....: " + hbtss);
					hbtss.close();
					log.info("Hearbeat task scheduler stream closed: " + hbtss);
				}
				var hbss = containeridhbss.get(cce.getContainerid());
				if (!Objects.isNull(hbss)) {
					log.info("Hearbeat closing....: " + hbtss);
					hbss.close();
					log.info("Hearbeat closed: " + hbtss);
				}
			} else if (deserobj instanceof RemoteDataFetch rdf) {
				log.info("Entering RemoteDataFetch: " + deserobj);
				var taskexecutor = jobstageexecutormap.get(rdf.getJobid() + rdf.getStageid() + rdf.getTaskid());
				var mdstde = (StreamPipelineTaskExecutor) taskexecutor;
				log.info("Executor: " + mdstde);
				if (rdf.getMode().equals(MDCConstants.STANDALONE)) {
					if (taskexecutor != null) {
						Task task  = mdstde.getTask();
						if (task.storage == MDCConstants.STORAGE.INMEMORY) {
							var os = ((StreamPipelineTaskExecutorInMemory) mdstde).getIntermediateInputStreamRDF(rdf);
							if (!Objects.isNull(os)) {
								ByteBufferOutputStream bbos = (ByteBufferOutputStream) os;
								rdf.setData(IOUtils.readBytesAndClose(new ByteBufferInputStream(bbos.get()), bbos.get().limit()));
							}
						} else if (task.storage == MDCConstants.STORAGE.INMEMORY_DISK) {
							var path = Utils.getIntermediateInputStreamRDF(rdf);
							rdf.setData((byte[]) inmemorycache.get(path));
						} else {
							try (var is = mdstde.getIntermediateInputStreamFS(task);) {
								rdf.setData((byte[]) is.readAllBytes());
							}
						}
						s.send(event.getCtx(), rdf);
					}
				}
				else if (rdf.getMode().equals(MDCConstants.JGROUPS)) {
					try (var is = RemoteDataFetcher.readIntermediatePhaseOutputFromFS(rdf.getJobid(), rdf.getTaskid());) {
						rdf.setData((byte[]) is.readAllBytes());
						s.send(event.getCtx(), rdf);
					}
				} else {
					if (taskexecutor != null) {
						try (var is = RemoteDataFetcher
								.readIntermediatePhaseOutputFromFS(rdf.getJobid(),
										mdstde.getIntermediateDataRDF(rdf.getTaskid()));) {
							rdf.setData((byte[]) is.readAllBytes());
							s.send(event.getCtx(), rdf);
						}
					}
				}

				log.info("Exiting RemoteDataFetch: ");
			} else if (deserobj instanceof List objects) {
				var object = objects.get(0);
				var applicationid = (String) objects.get(1);
				var taskid = (String) objects.get(2);
				{
					var apptaskid = applicationid + taskid;
					var taskexecutor = apptaskexecutormap.get(apptaskid);
					if (object instanceof BlocksLocation blockslocation) {
						var mdtemc = (TaskExecutorMapperCombiner) taskexecutor;
						if (taskexecutor == null || mdtemc.getHbts().taskstatus == TaskStatus.FAILED) {
							if (taskexecutor != null) {
								mdtemc.getHbts().stop();
								mdtemc.getHbts().destroy();
								apptaskexecutormap.remove(apptaskid);
							}
							try (var hdfs = FileSystem.newInstance(
									new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL)),
									configuration)) {
								log.debug("Application Submitted:" + applicationid + "-" + taskid);

								var hbts = hbtsappid.get(applicationid);
								mdtemc = new TaskExecutorMapperCombiner(blockslocation,
										HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs), applicationid, taskid, cl,
										port, hbts);
							}
							apptaskexecutormap.put(apptaskid, mdtemc);
							CompletableFuture.runAsync(mdtemc, es);
						}
					} else if (object instanceof ReducerValues rv) {
						var mdter = (TaskExecutorReducer) taskexecutor;
						if (taskexecutor == null || mdter.getHbts().taskstatus == TaskStatus.FAILED) {
							if (taskexecutor != null) {
								mdter.getHbts().stop();
								mdter.getHbts().destroy();
								apptaskexecutormap.remove(apptaskid);
							}
							var hbts = hbtsappid.get(applicationid);
							mdter = new TaskExecutorReducer(rv, applicationid, taskid, cl, port,
									hbts, apptaskexecutormap);
							apptaskexecutormap.put(apptaskid, mdter);
							log.debug("Reducer submission:" + apptaskid);
							CompletableFuture.runAsync(mdter, es);
						}
					} else if (object instanceof RetrieveData) {
						Context ctx = null;
						if (taskexecutor instanceof TaskExecutorReducer ter) {
							ter.getHbts().stop();
							ter.getHbts().destroy();
							log.info("Obtaining reducer Context: " + apptaskid);
							ctx = ter.ctx;
						} else if (taskexecutor instanceof TaskExecutorMapperCombiner temc) {
							temc.getHbts().stop();
							temc.getHbts().destroy();
							log.info("Obtaining reducer Context: " + apptaskid);
							ctx = temc.ctx;
						}
						s.send(event.getCtx(), ctx);
						apptaskexecutormap.remove(apptaskid);
					} else if (object instanceof RetrieveKeys rk) {
						var mdtemc = (TaskExecutorMapperCombiner) taskexecutor;
						var keys = mdtemc.ctx.keys();
						rk.keys = new LinkedHashSet<>(keys);
						rk.applicationid = applicationid;
						rk.taskid = taskid;
						rk.response = true;
						s.send(event.getCtx(), rk);
						mdtemc.getHbts().stop();
						mdtemc.getHbts().destroy();
						log.debug("destroying MapperCombiner HeartBeat: " + apptaskid);
					}
				}
			}
		} catch (Exception ex) {
			log.error("Job Execution Problem", ex);
		}

	}
}
