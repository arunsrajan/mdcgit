package com.github.mdc.tasks.executor;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.ehcache.Cache;
import org.xerial.snappy.SnappyInputStream;

import com.esotericsoftware.kryo.io.Input;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.ByteBufferInputStream;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.CacheAvailability;
import com.github.mdc.common.CacheUtils;
import com.github.mdc.common.CloseStagesGraphExecutor;
import com.github.mdc.common.Context;
import com.github.mdc.common.FreeResourcesCompletedJob;
import com.github.mdc.common.HeartBeatServerStream;
import com.github.mdc.common.HeartBeatTaskScheduler;
import com.github.mdc.common.HeartBeatTaskSchedulerStream;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.ReducerValues;
import com.github.mdc.common.RemoteDataFetch;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.RemoteDataWriterTask;
import com.github.mdc.common.RetrieveData;
import com.github.mdc.common.RetrieveKeys;
import com.github.mdc.common.Task;
import com.github.mdc.common.TasksGraphExecutor;
import com.github.mdc.common.Utils;
import com.github.mdc.common.ApplicationTask.TaskStatus;
import com.github.mdc.common.MDCConstants.STORAGE;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorJGroups;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutor;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorInMemory;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorInMemoryDisk;

public class TaskExecutor implements Runnable {
	private static Logger log = Logger.getLogger(TaskExecutor.class);
	Socket s;
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
	Map<String, HeartBeatServerStream> containeridhbss = new ConcurrentHashMap<>();
	Map<String, JobStage> jobidstageidjobstagemap;
	Queue<Object> taskqueue;

	@SuppressWarnings({ "rawtypes" })
	public TaskExecutor(Socket s, ClassLoader cl, int port, ExecutorService es, Configuration configuration,
			Map<String, Object> apptaskexecutormap, Map<String, Object> jobstageexecutormap,
			ConcurrentMap<String, OutputStream> resultstream, Cache inmemorycache, Object deserobj,
			Map<String, HeartBeatTaskScheduler> hbtsappid, Map<String, HeartBeatTaskSchedulerStream> hbtssjobid,
			Map<String, HeartBeatServerStream> containeridhbss,
			Map<String, Map<String, Object>> jobidstageidexecutormap, Queue<Object> taskqueue,
			Map<String, JobStage> jobidstageidjobstagemap) {
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
	}

	ClassLoader cl;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void run() {
		log.debug("Started the run------------------------------------------------------");
		try {
			log.debug("Framework Message: " + deserobj);
			if (deserobj instanceof JobStage jobstage) {
				jobidstageidjobstagemap.put(jobstage.jobid + jobstage.stageid, jobstage);
			} else if (deserobj instanceof Task task) {
				log.info("Received Job Stage: " + deserobj);
				var taskexecutor = jobstageexecutormap.get(task.jobid + task.stageid + task.taskid);
				var hbtss = hbtssjobid.get(task.jobid);
				var mdste = (StreamPipelineTaskExecutor) taskexecutor;
				if (taskexecutor == null || mdste.isCompleted() && task.taskstatus == Task.TaskStatus.FAILED) {
					var key = task.jobid + task.stageid;
					mdste = task.storage == STORAGE.INMEMORY_DISK?new StreamPipelineTaskExecutorInMemoryDisk(jobidstageidjobstagemap.get(key), resultstream,
							inmemorycache, hbtss):task.storage == STORAGE.INMEMORY?new StreamPipelineTaskExecutorInMemory(jobidstageidjobstagemap.get(key),
									resultstream, inmemorycache):new StreamPipelineTaskExecutor(jobidstageidjobstagemap.get(key),inmemorycache);
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
					taskqueue.offer(mdste);
					log.info("Submitted Job Stage for execution: " + deserobj);
				} else if (mdste.isCompleted()) {
					hbtss.setTimetakenseconds(mdste.getHbtss().getTimetakenseconds());
					hbtss.pingOnce(task.stageid, task.taskid, task.hostport, Task.TaskStatus.COMPLETED, mdste.getHbtss().getTimetakenseconds(), null);
				}
			} else if (deserobj instanceof TasksGraphExecutor stagesgraph) {
				var taskexecutor = jobstageexecutormap.get(stagesgraph.getTasks().get(0).jobid);
				var mdste = (StreamPipelineTaskExecutorJGroups) taskexecutor;
				if (taskexecutor == null) {
					mdste = new StreamPipelineTaskExecutorJGroups(jobidstageidjobstagemap, stagesgraph.getTasks(),
							port, inmemorycache);
					mdste.setExecutor(es);
					var key = stagesgraph.getTasks().get(0).jobid;
					jobstageexecutormap.put(key, mdste);
					taskqueue.offer(mdste);
				}
			} else if (deserobj instanceof CloseStagesGraphExecutor closestagesgraph) {
				var taskexecutor = jobstageexecutormap.get(closestagesgraph.getJobid());
				var mdste = (StreamPipelineTaskExecutorJGroups) taskexecutor;
				if (taskexecutor != null) {
					mdste.channel.close();
					jobstageexecutormap.remove(closestagesgraph.getJobid());
				}
			} else if (deserobj instanceof FreeResourcesCompletedJob cce) {
				var jsem = jobidstageidexecutormap.remove(cce.jobid);
				if (!Objects.isNull(jsem)) {
					var keys = jsem.keySet();
					for (var key : keys) {
						jsem.remove(key);
						jobstageexecutormap.remove(cce.jobid + key);
						log.info("Removed stages: " + cce.jobid + key);
					}
				}
				var hbtss = hbtssjobid.remove(cce.jobid);
				if (!Objects.isNull(hbtss)) {
					log.info("Hearbeat task scheduler stream closing....: " + hbtss);
					hbtss.close();
					log.info("Hearbeat task scheduler stream closed: " + hbtss);
				}
				var hbss = containeridhbss.get(cce.containerid);
				if (!Objects.isNull(hbss)) {
					log.info("Hearbeat closing....: " + hbtss);
					hbss.close();
					log.info("Hearbeat closed: " + hbtss);
				}
			} else if (deserobj instanceof RemoteDataFetch rdf) {
				log.info("Entering RemoteDataFetch: " + deserobj);
				var taskexecutor = jobstageexecutormap.get(rdf.jobid + rdf.stageid + rdf.taskid);
				var mdstde = (StreamPipelineTaskExecutor) taskexecutor;
				if (taskexecutor != null) {
					Task task  = mdstde.getTask();
					if(task.storage == MDCConstants.STORAGE.INMEMORY) {
						var os = ((StreamPipelineTaskExecutorInMemory)mdstde).getIntermediateInputStreamRDF(rdf);
						if (!Objects.isNull(os)) {
							rdf.data = ((ByteArrayOutputStream)os).toByteArray();
						}
					}else if(task.storage == MDCConstants.STORAGE.INMEMORY_DISK) {
						var path = Utils.getIntermediateInputStreamRDF(rdf);
						rdf.data = (byte[]) inmemorycache.get(path);
					}
					Utils.writeObjectByStream(s.getOutputStream(), rdf);
					s.close();
				}
				log.info("Exiting RemoteDataFetch: ");
			} else if (deserobj instanceof CacheAvailability ca) {
				var bl = ca.bl;
				ca.available = true;
				for (var block : bl.block) {
					var blockkey = block.filename + "-" + block.blockstart + "-" + block.blockend;
					if (!inmemorycache.containsKey(blockkey) || inmemorycache.get(blockkey) == null) {
						ca.available = false;
						break;
					}
				}
				ca.response = true;
				Utils.writeObjectByStream(s.getOutputStream(), ca);
				s.close();
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
							var hdfs = FileSystem.newInstance(
									new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
									configuration);
							log.debug("Application Submitted:" + applicationid + "-" + taskid);
							var taskpool = Executors.newWorkStealingPool();
							var hbts = hbtsappid.get(applicationid);
							mdtemc = new TaskExecutorMapperCombiner(blockslocation,
									CacheUtils.getBlockData(blockslocation, hdfs), applicationid, taskid, taskpool, cl,
									port, hbts);
							hdfs.close();
							apptaskexecutormap.put(apptaskid, mdtemc);
							taskpool.execute(mdtemc);
						}
					} else if (object instanceof ReducerValues rv) {
						var mdter = (TaskExecutorReducer) taskexecutor;
						if (taskexecutor == null || mdter.getHbts().taskstatus == TaskStatus.FAILED) {
							if (taskexecutor != null) {
								mdter.getHbts().stop();
								mdter.getHbts().destroy();
								apptaskexecutormap.remove(apptaskid);
							}
							var taskpool = Executors.newWorkStealingPool();
							var hbts = hbtsappid.get(applicationid);
							mdter = new TaskExecutorReducer(rv, applicationid, taskid, taskpool, cl, port,
									hbts);
							apptaskexecutormap.put(apptaskid, mdter);
							log.debug("Reducer submission:" + apptaskid);
							taskpool.execute(mdter);
						}
					} else if (object instanceof RetrieveData) {
						Context ctx = null;
						if (taskexecutor instanceof TaskExecutorReducer mdter) {
							mdter.getHbts().stop();
							mdter.getHbts().destroy();
							log.debug("Obtaining reducer Context: " + apptaskid);
							ctx = (Context) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(applicationid,
									(apptaskid + MDCConstants.DATAFILEEXTN), false);
						}
						Utils.writeObjectByStream(s.getOutputStream(), ctx);
						s.close();
						apptaskexecutormap.remove(apptaskid);
					} else if (object instanceof RetrieveKeys rk) {
						var keys = RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(applicationid,
								(apptaskid + MDCConstants.DATAFILEEXTN), true);
						rk.keys = (Set<Object>) keys;
						rk.applicationid = applicationid;
						rk.taskid = taskid;
						rk.response = true;
						Utils.writeObjectByStream(s.getOutputStream(), rk);
						s.close();
						if (taskexecutor instanceof TaskExecutorMapperCombiner mdtemc) {
							mdtemc.getHbts().stop();
							mdtemc.getHbts().destroy();
							log.debug("destroying MapperCombiner HeartBeat: " + apptaskid);
						}
					}
				}
			}
		} catch (Exception ex) {
			log.error("Job Execution Problem", ex);
		}

	}
}
