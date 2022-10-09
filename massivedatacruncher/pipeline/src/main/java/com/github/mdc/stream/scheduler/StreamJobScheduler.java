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
package com.github.mdc.stream.scheduler;

import static java.util.Objects.nonNull;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.lang.ref.SoftReference;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.log4j.Logger;
import org.ehcache.Cache;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.jgroups.JChannel;
import org.jgroups.ObjectMessage;
import org.jooq.lambda.tuple.Tuple2;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.nustaq.serialization.FSTSerialisationListener;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.client.CommandYarnClient;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.github.dexecutor.core.DefaultDexecutor;
import com.github.dexecutor.core.DexecutorConfig;
import com.github.dexecutor.core.ExecutionConfig;
import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.TaskProvider;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.ByteBufferInputStream;
import com.github.mdc.common.ByteBufferOutputStream;
import com.github.mdc.common.CloseStagesGraphExecutor;
import com.github.mdc.common.ContainerResources;
import com.github.mdc.common.DAGEdge;
import com.github.mdc.common.DestroyContainer;
import com.github.mdc.common.DestroyContainers;
import com.github.mdc.common.FileSystemSupport;
import com.github.mdc.common.FreeResourcesCompletedJob;
import com.github.mdc.common.GlobalContainerAllocDealloc;
import com.github.mdc.common.HeartBeatStream;
import com.github.mdc.common.Job;
import com.github.mdc.common.JobApp;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.LoadJar;
import com.github.mdc.common.MDCCache;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCNodesResources;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.PipelineConstants;
import com.github.mdc.common.RemoteDataFetch;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.Resources;
import com.github.mdc.common.Stage;
import com.github.mdc.common.Task;
import com.github.mdc.common.TasksGraphExecutor;
import com.github.mdc.common.TssHAChannel;
import com.github.mdc.common.TssHAHostPorts;
import com.github.mdc.common.Utils;
import com.github.mdc.common.WhoIsResponse;
import com.github.mdc.common.functions.AggregateReduceFunction;
import com.github.mdc.common.functions.Coalesce;
import com.github.mdc.common.functions.IntersectionFunction;
import com.github.mdc.common.functions.Join;
import com.github.mdc.common.functions.JoinPredicate;
import com.github.mdc.common.functions.LeftJoin;
import com.github.mdc.common.functions.LeftOuterJoinPredicate;
import com.github.mdc.common.functions.RightJoin;
import com.github.mdc.common.functions.RightOuterJoinPredicate;
import com.github.mdc.common.functions.UnionFunction;
import com.github.mdc.stream.PipelineException;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutor;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorIgnite;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorLocal;
import com.github.mdc.stream.mesos.scheduler.MesosScheduler;
import com.google.common.collect.Iterables;

/**
 * 
 * @author Arun Schedule the jobs for parallel execution to task executor for
 *         standalone application or mesos scheduler and executor or yarn
 *         scheduler i.e app master and executor.
 */
public class StreamJobScheduler {

	private static Logger log = Logger.getLogger(StreamJobScheduler.class);

	public int batchsize;
	public Set<String> taskexecutors;
	private Semaphore yarnmutex = new Semaphore(1);
	public ConcurrentMap<String, OutputStream> resultstream;
	@SuppressWarnings("rawtypes")
	Cache cache;
	public Semaphore semaphore;
	public HeartBeatStream hbss;
	
	public PipelineConfig pipelineconfig;
	AtomicBoolean istaskcancelled = new AtomicBoolean();
	public Map<String, JobStage> jsidjsmap = new ConcurrentHashMap<>();
	public List<Object> stageoutput = new ArrayList<>();
	String hdfsfilepath;
	FileSystem hdfs;
	String hbphysicaladdress;

	public StreamJobScheduler() {
		hdfsfilepath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL_DEFAULT);
	}

	ExecutorService jobping = Executors.newWorkStealingPool();
	public Job job;
	Boolean islocal;
	public Boolean isignite;
	JChannel chtssha;

	/**
	 * Schedule the job for parallelization
	 * 
	 * @param job
	 * @return
	 * @throws Exception
	 * @throws Throwable
	 */
	@SuppressWarnings({"unchecked", "rawtypes", "resource"})
	public Object schedule(Job job) throws Exception {
		this.job = job;
		this.pipelineconfig = job.getPipelineconfig();
		// If scheduler is mesos?
		var ismesos = Boolean.parseBoolean(pipelineconfig.getMesos());
		// If scheduler is yarn?
		var isyarn = Boolean.parseBoolean(pipelineconfig.getYarn());
		// If scheduler is local?
		islocal = Boolean.parseBoolean(pipelineconfig.getLocal());
		// If scheduler is JGroups
		var isjgroups = Boolean.parseBoolean(pipelineconfig.getJgroups());
		// If ignite mode is choosen
		isignite = Objects.isNull(pipelineconfig.getMode()) ? false
				: pipelineconfig.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;

		try (var hbss = new HeartBeatStream();
				var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());) {
			this.hdfs = hdfs;
			this.hbss = hbss;
			TasksGraphExecutor[] tasksgraphexecutor = null;
			// If not yarn and mesos start the resources and task scheduler
			// heart beats
			// for standalone MDC task schedulers and executors to communicate
			// via task statuses.
			if (Boolean.FALSE.equals(ismesos) && Boolean.FALSE.equals(isyarn) && Boolean.FALSE.equals(islocal)
					&& Boolean.FALSE.equals(isjgroups) && !isignite) {
				// Initialize the heart beat for gathering the resources
				// Initialize the heart beat for gathering the task executors
				// task statuses information.
				getContainersHostPort();
				batchsize = Integer.parseInt(pipelineconfig.getBatchsize());
				var taskexecutorssize = taskexecutors.size();
				taskexecutorssize = taskexecutors.size();
				log.debug("taskexecutorssize: " + taskexecutorssize);
				Utils.writeToOstream( pipelineconfig.getOutput(), "taskexecutorssize: " + taskexecutorssize);
				boolean haenabled = Boolean.parseBoolean(pipelineconfig.getTsshaenabled());
				if (!Objects.isNull(pipelineconfig.getJar()) && haenabled) {
					TssHAHostPorts.get().forEach(hp -> {
						try {
							LoadJar lj = new LoadJar();
							lj.setMrjar(pipelineconfig.getJar());
							String jarloaded = (String) Utils.getResultObjectByInput(hp, lj);
							if (!Objects.isNull(jarloaded) && jarloaded.equals(MDCConstants.JARLOADED)) {
								log.info("Jar Loaded in server " + hp);
							} else {
								log.info("Jar Not Loaded in server " + hp);
							}

						} catch (Exception e) {
							log.error(MDCConstants.EMPTY, e);
						}
					});
				}
				if (haenabled) {
					istaskcancelled.set(false);
					ping(job);
				}
			} else if (Boolean.TRUE.equals(isjgroups) && !isignite) {
				getContainersHostPort();
			}
			var uniquestagestoprocess = new ArrayList<>(job.getTopostages());
			var stagenumber = 0;
			var graph = new SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge>(DAGEdge.class);
			var taskgraph = new SimpleDirectedGraph<Task, DAGEdge>(DAGEdge.class);
			// Generate Physical execution plan for each stages.
			if (Objects.isNull(job.getVertices())) {
				for (var stage : uniquestagestoprocess) {
					JobStage js = new JobStage();
					js.setJobid(job.getId());
					js.setStageid(stage.id);
					js.setStage(stage);
					jsidjsmap.put( job.getId() + stage.id, js);
					partitionindex = 0;
					var nextstage = stagenumber + 1 < uniquestagestoprocess.size()
							? uniquestagestoprocess.get(stagenumber + 1)
							: null;
					stage.number = stagenumber;
					generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(),  job.getId(), graph, taskgraph);
					stagenumber++;
				}
				job.setVertices(new LinkedHashSet<>(graph.vertexSet()));
				job.setEdges(new LinkedHashSet<>(graph.edgeSet()));
			} else {
				job.getVertices().stream().forEach(vertex -> graph.addVertex((StreamPipelineTaskSubmitter) vertex));
				job.getEdges().stream()
						.forEach(edge -> graph.addEdge((StreamPipelineTaskSubmitter) edge.getSource(),
								(StreamPipelineTaskSubmitter) edge.getTarget()));
			}
			batchsize = Integer.parseInt(pipelineconfig.getBatchsize());
			semaphore = new Semaphore(batchsize);
			var writer = new StringWriter();
			if (Boolean.parseBoolean((String) MDCProperties.get().get(MDCConstants.GRAPHSTOREENABLE))) {
				Utils.renderGraphPhysicalExecPlan(taskgraph, writer);
			}
			// Topological ordering of the job stages in
			// MassiveDataStreamTaskSchedulerThread is processed.
			Iterator<StreamPipelineTaskSubmitter> topostages = new TopologicalOrderIterator(graph);
			var mdststs = new ArrayList<StreamPipelineTaskSubmitter>();
			// Sequential ordering of topological ordering is obtained to
			// process for parallelization.
			while (topostages.hasNext()) {
				mdststs.add(topostages.next());
			}
			log.debug(mdststs);
			var mdstts = getFinalPhasesWithNoSuccessors(graph, mdststs);
			var partitionnumber = 0;
			var ishdfs = false;
			if(nonNull(job.getUri())) {
				ishdfs = new URL(job.getUri()).getProtocol().equals(MDCConstants.HDFS_PROTOCOL);
			}
			for (var mdstst : mdstts) {
				mdstst.getTask().finalphase = true;
				if (job.getTrigger() == job.getTrigger().SAVERESULTSTOFILE && ishdfs) {
					mdstst.getTask().saveresulttohdfs = true;
					mdstst.getTask().hdfsurl = job.getUri();
					mdstst.getTask().filepath = job.getSavepath() + MDCConstants.HYPHEN + partitionnumber++;
				}
			}
			Utils.writeToOstream( pipelineconfig.getOutput(), "stages: " + mdststs);
			if (isignite) {
				parallelExecutionPhaseIgnite(graph, new TaskProviderIgnite());
			}
			// If local scheduler
			else if (Boolean.TRUE.equals(islocal)) {
				resultstream = new ConcurrentHashMap<>();
				batchsize = Integer.parseInt(pipelineconfig.getBatchsize());
				semaphore = new Semaphore(batchsize);
				cache = MDCCache.get();
				parallelExecutionPhaseDExecutorLocalMode(graph, new TaskProviderLocalMode(graph.vertexSet().size()));
			}
			// If mesos is scheduler run mesos framework.
			else if (Boolean.TRUE.equals(ismesos)) {
				MesosScheduler.runFramework(mdststs, graph, MDCProperties.get().getProperty(MDCConstants.MESOS_MASTER),
						taskmdsthread, pipelineconfig.getJar());
			}
			// If Yarn is scheduler run yarn scheduler via spring yarn
			// framework.
			else if (Boolean.TRUE.equals(isyarn)) {
				yarnmutex.acquire();
				new File(MDCConstants.LOCAL_FS_APPJRPATH).mkdirs();
				Utils.createJar(new File(MDCConstants.YARNFOLDER), MDCConstants.LOCAL_FS_APPJRPATH,
						MDCConstants.YARNOUTJAR);
				var yarninputfolder = MDCConstants.YARNINPUTFOLDER + MDCConstants.FORWARD_SLASH +  job.getId();
				RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(mdststs, yarninputfolder,
						MDCConstants.MASSIVEDATA_YARNINPUT_DATAFILE);
				RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(graph, yarninputfolder,
						MDCConstants.MASSIVEDATA_YARNINPUT_GRAPH_FILE);
				RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(taskmdsthread, yarninputfolder,
						MDCConstants.MASSIVEDATA_YARNINPUT_TASK_FILE);
				RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(jsidjsmap, yarninputfolder,
						MDCConstants.MASSIVEDATA_YARNINPUT_JOBSTAGE_FILE);
				decideContainerCountAndPhysicalMemoryByBlockSize(mdststs.size(),
						Integer.parseInt(pipelineconfig.getBlocksize()));
				ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
						MDCConstants.FORWARD_SLASH + YarnSystemConstants.DEFAULT_CONTEXT_FILE_CLIENT, getClass());
				var client = (CommandYarnClient) context.getBean(MDCConstants.YARN_CLIENT);
				client.getEnvironment().put(MDCConstants.YARNMDCJOBID,  job.getId());
				var appid = client.submitApplication(true);
				var appreport = client.getApplicationReport(appid);
				yarnmutex.release();
				while (appreport.getYarnApplicationState() != YarnApplicationState.FINISHED
						&& appreport.getYarnApplicationState() != YarnApplicationState.FAILED) {
					appreport = client.getApplicationReport(appid);
					Thread.sleep(1000);
				}
			}
			// If Jgroups is the scheduler;
			else if (Boolean.TRUE.equals(isjgroups)) {
				Iterator<Task> toposort = new TopologicalOrderIterator(taskgraph);
				var tasks = new ArrayList<Task>();
				taskexecutors = new LinkedHashSet<>();
				while (toposort.hasNext()) {
					var task = toposort.next();
					taskexecutors.add(task.hostport);
					tasks.add(task);
				}
				tasksgraphexecutor = new TasksGraphExecutor[taskexecutors.size()];
				var taskgraphexecutormap = new ConcurrentHashMap<String, TasksGraphExecutor>();
				var stagegraphexecutorindex = 0;
				broadcastJobStageToTaskExecutors();
				for (var te : taskexecutors) {
					tasksgraphexecutor[stagegraphexecutorindex] = new TasksGraphExecutor();
					tasksgraphexecutor[stagegraphexecutorindex].setStages(new ArrayList<>());
					tasksgraphexecutor[stagegraphexecutorindex].setHostport(te);
					taskgraphexecutormap.put(te, tasksgraphexecutor[stagegraphexecutorindex]);
					stagegraphexecutorindex++;
				}
				stagegraphexecutorindex = 0;
				Task tasktmp = null;
				var rand = new Random(System.currentTimeMillis());
				var taskhpmap = new ConcurrentHashMap<Task, String>();
				toposort = new TopologicalOrderIterator(taskgraph);
				// Sequential ordering of topological ordering is obtained to
				// process for parallelization.
				while (toposort.hasNext()) {
					tasktmp = toposort.next();
					var taskspredecessor = Graphs.predecessorListOf(taskgraph, tasktmp);
					tasktmp.taskspredecessor = taskspredecessor;
					taskhpmap.put(tasktmp, tasktmp.hostport);
					if (taskspredecessor.isEmpty()) {
						var taskgraphexecutor = taskgraphexecutormap.get(tasktmp.hostport);
						taskgraphexecutor.getTasks().add(tasktmp);
					} else {
						var tasktoadd = taskspredecessor.get(rand.nextInt(taskspredecessor.size()));
						var taskgraphexecutor = taskgraphexecutormap.get(taskhpmap.get(tasktoadd));
						taskgraphexecutor.getTasks().add(tasktmp);
					}
				}
				log.debug(Arrays.asList(tasksgraphexecutor));
				for (var stagesgraphexecutor : tasksgraphexecutor) {
					if (!stagesgraphexecutor.getTasks().isEmpty()) {
						Utils.getResultObjectByInput(stagesgraphexecutor.getHostport(), stagesgraphexecutor);
					}
				}
				var stagepartids = tasks.parallelStream().map(taskpart -> taskpart.taskid).collect(Collectors.toSet());
				var stagepartidstatusmapresp = new ConcurrentHashMap<String, WhoIsResponse.STATUS>();
				var stagepartidstatusmapreq = new ConcurrentHashMap<String, WhoIsResponse.STATUS>();
				try (var channel = Utils.getChannelTaskExecutor( job.getId(),
						NetworkUtil.getNetworkAddress(
								MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST)),
						Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT)),
						stagepartidstatusmapreq, stagepartidstatusmapresp);) {
					var totaltasks = tasks.size();
					while (true) {
						Utils.whoare(channel);
						var totalcompleted = 0.0;
						for (var stagepart : stagepartids) {
							if (stagepartidstatusmapresp.get(stagepart) == WhoIsResponse.STATUS.COMPLETED) {
								totalcompleted++;
							}
						}
						double percentagecompleted = Math.floor((totalcompleted / totaltasks) * 100.0);
						if (totalcompleted == totaltasks) {
							Utils.writeToOstream( pipelineconfig.getOutput(), "\nPercentage Completed "
									+ percentagecompleted + "% \n");
							job.getJm().getContainersallocated().put("", percentagecompleted);
							mdststs.parallelStream().forEach(spts -> spts.setCompletedexecution(true));
							break;
						} else {
							log.debug(
									"Total Percentage Completed: " + Math.floor((totalcompleted / totaltasks) * 100.0));
							Utils.writeToOstream( pipelineconfig.getOutput(), "\nPercentage Completed "
									+ percentagecompleted + "% \n");
							job.getJm().getContainersallocated().put("", percentagecompleted);
							Thread.sleep(4000);
						}
					}
					stagepartidstatusmapresp.clear();
					stagepartidstatusmapreq.clear();					
				} catch (InterruptedException e) {
					log.warn("Interrupted!", e);
					// Restore interrupted state...
					Thread.currentThread().interrupt();
				} catch (Exception e) {
					log.error(MDCConstants.EMPTY, e);
				}
			}
			// If not yarn or mesos schedule via standalone task executors
			// daemon.
			else {
				broadcastJobStageToTaskExecutors();
				parallelExecutionPhaseDExecutor(graph);
			}

			// Obtain the final stage job results after final stage is
			// completed.
			var finalstageoutput = getLastStageOutput(mdstts, graph, mdststs, ismesos, isyarn, islocal, isjgroups,
					resultstream);
			if(Boolean.TRUE.equals(isjgroups)) {
				closeResourcesTaskExecutor(tasksgraphexecutor);
			}
			job.setIscompleted(true);
			job.getJm().setJobcompletiontime(System.currentTimeMillis());
			Utils.writeToOstream( pipelineconfig.getOutput(),
					"Completed Job in " + ((job.getJm().getJobcompletiontime() - job.getJm().getJobstarttime()) / 1000.0) + " seconds");
			log.info("Completed Job in " + ((job.getJm().getJobcompletiontime() - job.getJm().getJobstarttime()) / 1000.0) + " seconds");
			job.getJm().setTotaltimetaken((job.getJm().getJobcompletiontime() - job.getJm().getJobstarttime()) / 1000.0);
			Utils.writeToOstream( pipelineconfig.getOutput(),
					"Job Metrics " + job.getJm());
			log.info("Job Metrics " + job.getJm());
			if (Boolean.TRUE.equals(islocal)) {
				var srresultstore = new SoftReference<ConcurrentMap<String, OutputStream>>(resultstream);
				srresultstore.clear();
				resultstream = null;
			}
			return finalstageoutput;
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
			return null;
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERERROR, ex);
		} finally {
			if (!Objects.isNull(job.getIgcache())) {
				job.getIgcache().close();
			}
			if ((Boolean.FALSE.equals(ismesos) && Boolean.FALSE.equals(isyarn) && Boolean.FALSE.equals(islocal)
					|| Boolean.TRUE.equals(isjgroups)) && !isignite) {
				if (!pipelineconfig.getUseglobaltaskexecutors()) {
					if (!Boolean.TRUE.equals(isjgroups)) {
						var cce = new FreeResourcesCompletedJob();
						cce.setJobid( job.getId());
						cce.setContainerid(job.getContainerid());
						for (var te : taskexecutors) {
							Utils.getResultObjectByInput(te, cce);
						}
					}
					destroyContainers();
				}
			}
			if (!Objects.isNull(hbss)) {
				hbss.close();
			}
			if (!Objects.isNull(job.getAllstageshostport())) {
				job.getAllstageshostport().clear();
			}
			if (!Objects.isNull(job.getStageoutputmap())) {
				job.getStageoutputmap().clear();
			}
			istaskcancelled.set(true);
			jobping.shutdownNow();
		}

	}

	public void broadcastJobStageToTaskExecutors() throws Exception {
		jsidjsmap.keySet().stream().forEach(key -> {
			for (String te : taskexecutors) {
				try {
					JobStage js = (JobStage) jsidjsmap.get(key);
					FSTConfiguration conf = Utils.getConfigForSerialization();
					try(var baos = new ByteArrayOutputStream(); 
							var output = new FSTObjectOutput(baos, conf)){
						output.setListener(new FSTSerialisationListener() {
							
							@Override
							public void objectWillBeWritten(Object obj, int streamPosition) {
								log.info(obj.getClass());
							}
							
							@Override
							public void objectHasBeenWritten(Object obj, int oldStreamPosition, int streamPosition) {
								log.info(obj.getClass());
								
							}
						});
						output.writeObject(js);
						output.flush();
						Utils.getResultObjectByInput(te, baos.toByteArray());
					}
				} catch (Exception e) {
					log.error(MDCConstants.EMPTY, e);
				}
			}
		});
	}

	/**
	 * Get containers host port by launching.
	 *
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public void getContainersHostPort() throws PipelineException {
		try {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().acquire();
			hbss.init(
					Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY)),
					Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT)),
					NetworkUtil
							.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST)),
					Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_INITIALDELAY)),
					Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PINGDELAY)),
					job.getContainerid());
			// Start Resources gathering via heart beat resources status update.
			hbss.start();
			var loadjar = new LoadJar();
			loadjar.setMrjar(pipelineconfig.getJar());
			if(nonNull(pipelineconfig.getCustomclasses()) && !pipelineconfig.getCustomclasses().isEmpty()) {
				loadjar.setClasses(pipelineconfig.getCustomclasses().stream().map(clz->clz.getName()).collect(Collectors.toCollection(LinkedHashSet::new)));
			}
			for (var lc : job.getLcs()) {
				List<Integer> ports = null;
				if (pipelineconfig.getUseglobaltaskexecutors()) {
					ports = lc.getCla().getCr().stream().map(cr -> {
						return cr.getPort();
					}).collect(Collectors.toList());
				} else {
					ports =	(List<Integer>) Utils.getResultObjectByInput(lc.getNodehostport(), lc);
				}
				int index = 0;
				String tehost = lc.getNodehostport().split("_")[0];
				while (index < ports.size()) {
					while (true) {
						try (Socket sock = new Socket(tehost, ports.get(index))) {
							break;
						} catch (Exception ex) {
							Thread.sleep(200);
						}
					}
					if (!Objects.isNull(loadjar.getMrjar())) {
						log.info(Utils.getResultObjectByInput(tehost+MDCConstants.UNDERSCORE+ports.get(index), loadjar));
					}
					JobApp jobapp = new JobApp();
					jobapp.setContainerid(lc.getContainerid());
					jobapp.setJobappid( job.getId());
					jobapp.setJobtype(JobApp.JOBAPP.STREAM);
					Utils.getResultObjectByInput(tehost + MDCConstants.UNDERSCORE + ports.get(index), jobapp);
					index++;
				}
			}			
			taskexecutors = new LinkedHashSet<>(hbss.containers);
			while (taskexecutors.size() != job.getContainers().size()) {
				taskexecutors = new LinkedHashSet<>(hbss.containers);
				Thread.sleep(500);
			}
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERCONTAINERERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERCONTAINERERROR, ex);
		} finally {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().release();
		}
	}

	/**
	 * Destroys all the allocated container allocated to current job.
	 * 
	 * @throws Exception
	 */
	public void destroyContainers() throws PipelineException {
		try {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().acquire();
			if (!Objects.isNull(job.getNodes())) {
				var nodes = job.getNodes();
				var contcontainerids = GlobalContainerAllocDealloc.getContainercontainerids();
				var chpcres = GlobalContainerAllocDealloc.getHportcrs();
				var deallocateall = true;
				if (!Objects.isNull(job.getContainers())) {
					for (String container : job.getContainers()) {
						var cids = contcontainerids.get(container);
						cids.remove(job.getContainerid());
						if (cids.isEmpty()) {
							contcontainerids.remove(container);
							var dc = new DestroyContainer();
							dc.setContainerid(job.getContainerid());
							dc.setContainerhp(container);
							String node = GlobalContainerAllocDealloc.getContainernode().remove(container);
							Set<String> containers = GlobalContainerAllocDealloc.getNodecontainers().get(node);
							containers.remove(container);
							Utils.getResultObjectByInput(node, dc);
							ContainerResources cr = chpcres.remove(container);
							Resources allocresources = MDCNodesResources.get().get(node);
							long maxmemory = cr.getMaxmemory() * MDCConstants.MB;
							long directheap = cr.getDirectheap() *  MDCConstants.MB;
							allocresources.setFreememory(allocresources.getFreememory()+maxmemory+directheap);
							allocresources.setNumberofprocessors(allocresources.getNumberofprocessors()+cr.getCpu());
						} else {
							deallocateall = false;
						}
					}
				}
				if (deallocateall) {
					var dc = new DestroyContainers();
					dc.setContainerid(job.getContainerid());
					log.debug("Destroying Containers with id:" + job.getContainerid() + " for the hosts: " + nodes);
					for (var node : nodes) {
						Utils.getResultObjectByInput(node, dc);
					}
				}
			}
		} catch (Exception ex) {
			log.error(PipelineConstants.DESTROYCONTAINERERROR, ex);
			throw new PipelineException(PipelineConstants.DESTROYCONTAINERERROR, ex);
		} finally {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().release();
		}
	}

	/**
	 * Closes Stages in task executor making room for another tasks to be executed
	 * in task executors JVM.
	 * 
	 * @param tasksgraphexecutor
	 * @throws Exception
	 */
	private void closeResourcesTaskExecutor(TasksGraphExecutor[] tasksgraphexecutor) throws Exception {
		for (var taskgraphexecutor : tasksgraphexecutor) {
			if (!taskgraphexecutor.getTasks().isEmpty()) {
				var hp = taskgraphexecutor.getHostport();
				if((boolean) Utils.getResultObjectByInput(hp, new CloseStagesGraphExecutor(taskgraphexecutor.getTasks()))) {
					log.info("Cleanup of tasks completed successfully for host "+hp);				}
			}
		}
	}

	/**
	 * Creates DExecutor object and executes tasks.
	 * 
	 * @param graph
	 * @param taskprovider
	 * @throws Exception
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void parallelExecutionPhaseDExecutorLocalMode(Graph<StreamPipelineTaskSubmitter, DAGEdge> graph,
			TaskProvider taskprovider) throws Exception {
		var es = newExecutor(batchsize);
		try {

			var config = new DexecutorConfig<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutor>(es,
					taskprovider);
			var executor = new DefaultDexecutor<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutor>(
					config);
			var edges = graph.edgeSet();
			if (!edges.isEmpty()) {
				for (var edge : edges) {
					executor.addDependency((StreamPipelineTaskSubmitter) edge.getSource(),
							(StreamPipelineTaskSubmitter) edge.getTarget());
				}
			} else {
				var vertices = graph.vertexSet();
				for (var vertex : vertices)
					executor.addDependency(vertex, vertex);
			}
			executor.execute(ExecutionConfig.NON_TERMINATING);
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
		} finally {
			if (!Objects.isNull(es)) {
				es.shutdownNow();
				es.awaitTermination(1, TimeUnit.SECONDS);
			}
		}
	}

	/**
	 * Creates DExecutor object and executes tasks in ignite server.
	 * 
	 * @param graph
	 * @param taskprovider
	 * @throws Exception
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void parallelExecutionPhaseIgnite(Graph<StreamPipelineTaskSubmitter, DAGEdge> graph,
			TaskProvider taskprovider) throws Exception {
		ExecutorService es = null;
		try {
			es = newExecutor(batchsize);
			var config = new DexecutorConfig<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutor>(es,
					taskprovider);
			var executor = new DefaultDexecutor<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutor>(
					config);
			var edges = graph.edgeSet();
			if (!edges.isEmpty()) {
				for (var edge : edges) {
					executor.addDependency((StreamPipelineTaskSubmitter) edge.getSource(),
							(StreamPipelineTaskSubmitter) edge.getTarget());
				}
			} else {
				var vertices = graph.vertexSet();
				for (var vertex : vertices)
					executor.addDependency(vertex, vertex);
			}
			executor.execute(ExecutionConfig.NON_TERMINATING);
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
		} finally {
			if (!Objects.isNull(es)) {
				es.shutdownNow();
				es.awaitTermination(1, TimeUnit.SECONDS);
			}
		}
	}


	Map<String, Integer> servertotaltasks = new ConcurrentHashMap<>();
	Map<String, Double> tetotaltaskscompleted = new ConcurrentHashMap<>();
	Map<String, Double> tetotaltasksfailed = new ConcurrentHashMap<>();

	/**
	 * Creates DExecutor object, executes tasks and reexecutes tasks if fails.
	 * 
	 * @param graph
	 * @throws Exception
	 */
	public void parallelExecutionPhaseDExecutor(Graph<StreamPipelineTaskSubmitter, DAGEdge> graph)
			throws Exception {
		ExecutorService es = null;
		try {
			var completed = false;
			var numexecute = 0;
			var executioncount = Integer.parseInt(pipelineconfig.getExecutioncount());
			batchsize = 0;
			List<ExecutionResult<StreamPipelineTaskSubmitter, Boolean>> erroredresult = null;

			var temdstdtmap = new ConcurrentHashMap<String, List<StreamPipelineTaskSubmitter>>();
			var chpcres = GlobalContainerAllocDealloc.getHportcrs();
			var semaphores = new ConcurrentHashMap<String, Semaphore>();
			for(var cr:chpcres.entrySet()) {
				batchsize += cr.getValue().getCpu()*2;
				semaphores.put(cr.getKey(), new Semaphore(cr.getValue().getCpu()*2));
			}
			es = newExecutor(batchsize);
			
			while (!completed && numexecute < executioncount) {
				temdstdtmap.clear();
				var configexec = new DexecutorConfig<StreamPipelineTaskSubmitter, Boolean>(es,
						new DAGScheduler(graph.vertexSet().size(), semaphores));
				var dexecutor = new DefaultDexecutor<StreamPipelineTaskSubmitter, Boolean>(configexec);

				var vertices = graph.vertexSet();
				List<StreamPipelineTaskSubmitter> mdststs = null;
				for (var mdstst : vertices) {
					var predecessors = Graphs.predecessorListOf(graph, mdstst);
					if (predecessors.size() > 0) {
						for (var pred : predecessors) {
							dexecutor.addDependency(pred, mdstst);
							log.info(pred + "->" + mdstst);
						}
					} else {
						dexecutor.addIndependent(mdstst);
					}
					if (Objects.isNull(servertotaltasks.get(mdstst.getHostPort()))) {
						servertotaltasks.put(mdstst.getHostPort(), 1);
					} else {
						servertotaltasks.put(mdstst.getHostPort(), servertotaltasks.get(mdstst.getHostPort()) + 1);
					}
				}
				var executionresultscomplete = dexecutor.execute(ExecutionConfig.NON_TERMINATING);
				erroredresult = executionresultscomplete.getErrored();
				log.info("Errored Tasks: " + erroredresult);
				if (erroredresult.isEmpty()) {
					completed = true;
				} else {
					var currentcontainers = new ArrayList<>(hbss.containers);
					var initialcontainers = new ArrayList<>(this.taskexecutors);
					initialcontainers.removeAll(currentcontainers);
					reformDAG(graph, currentcontainers, initialcontainers);
				}

				numexecute++;
			}
			Utils.writeToOstream( pipelineconfig.getOutput(), "Number of Executions: " + numexecute);
			if (!completed) {
				StringBuilder sb = new StringBuilder();
				if (erroredresult != null) {
					erroredresult.forEach(exec -> {
						sb.append(MDCConstants.NEWLINE);
						sb.append(exec.getId().getTask().stagefailuremessage);
					});
					if (!sb.isEmpty()) {
						throw new PipelineException(PipelineConstants.ERROREDTASKS.replace("%s", "" + executioncount) + sb.toString());
					}
				}
			}
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
		} finally {
			if (!Objects.isNull(es)) {
				es.shutdownNow();
				es.awaitTermination(1, TimeUnit.SECONDS);
			}
		}
	}

	/**
	 * Creates Executors
	 *
	 * @return ExecutorsService object.
	 */
	private ExecutorService newExecutor(int numberoftasks) {
		return Executors.newFixedThreadPool(numberoftasks);
	}

	/**
	 * Reformation of DAG if task executors are down.
	 * 
	 * @param graph
	 * @param availablecontainers
	 * @param destroyedcontainers
	 */
	public void reformDAG(Graph<StreamPipelineTaskSubmitter, DAGEdge> graph, List<String> availablecontainers,
			List<String> destroyedcontainers) {
		var toposort = new TopologicalOrderIterator<StreamPipelineTaskSubmitter, DAGEdge>(graph);
		var reversegraph = new EdgeReversedGraph<StreamPipelineTaskSubmitter, DAGEdge>(graph);
		for (; toposort.hasNext(); ) {
			var mdstst = (StreamPipelineTaskSubmitter) toposort.next();
			if (Graphs.predecessorListOf(graph, mdstst).isEmpty()) {
				if (destroyedcontainers.contains(mdstst.getHostPort())) {
					var hp = mdstst.getHostPort();
					reconfigureChildStages(mdstst, hp, Graphs.successorListOf(graph, mdstst), graph,
							availablecontainers, reversegraph);
					if (!mdstst.isCompletedexecution()) {
						reConfigureContainerForStageExecution(mdstst, availablecontainers);
						reconfigureChildStages(mdstst, hp, Graphs.successorListOf(graph, mdstst), graph);
					}
				}
			}
		}
	}

	/**
	 * Reconfigures child stages in graph.
	 * 
	 * @param mdstst
	 * @param hostport
	 * @param successors
	 * @param graph
	 * @param availablecontainers
	 * @param reversegraph
	 */
	public void reconfigureChildStages(StreamPipelineTaskSubmitter mdstst, String hostport,
			List<StreamPipelineTaskSubmitter> successors,
			Graph<StreamPipelineTaskSubmitter, DAGEdge> graph, List<String> availablecontainers,
			EdgeReversedGraph<StreamPipelineTaskSubmitter, DAGEdge> reversegraph) {
		for (var successor : successors) {
			if (successor.getHostPort().equals(hostport)) {
				if (!successor.isCompletedexecution()) {
					fillExecutionNotCompletedChildToParent(reversegraph, hostport, successor,
							Graphs.successorListOf(reversegraph, successor));
				} else {
					reconfigureChildStages(mdstst, hostport, Graphs.successorListOf(graph, successor), graph,
							availablecontainers, reversegraph);
				}
			} else if (!successor.isCompletedexecution()) {
				fillExecutionNotCompletedChildToParent(reversegraph, hostport, successor,
						Graphs.successorListOf(reversegraph, successor));
			}
		}
	}

	/**
	 * Reconfigures child stages in graph.
	 * 
	 * @param mdstst
	 * @param hostport
	 * @param successors
	 * @param graph
	 */
	public void reconfigureChildStages(StreamPipelineTaskSubmitter mdstst, String hostport,
			List<StreamPipelineTaskSubmitter> successors,
			Graph<StreamPipelineTaskSubmitter, DAGEdge> graph) {
		for (var successor : successors) {
			if (successor.getHostPort().equals(hostport)) {
				successor.setHostPort(mdstst.getHostPort());
				successor.getTask().hostport = mdstst.getHostPort();
				reConfigureParentRDFForStageExecution(successor, mdstst);
				reconfigureChildStages(successor, hostport, Graphs.successorListOf(graph, successor), graph);
			} else {
				reConfigureParentRDFForStageExecution(successor, mdstst);
			}
		}
	}

	/**
	 * Child to parent task not completed.
	 * 
	 * @param reversegraph
	 * @param hostport
	 * @param parent
	 * @param successors
	 */
	public void fillExecutionNotCompletedChildToParent(
			EdgeReversedGraph<StreamPipelineTaskSubmitter, DAGEdge> reversegraph, String hostport,
			StreamPipelineTaskSubmitter parent, List<StreamPipelineTaskSubmitter> successors) {
		for (var successor : successors) {
			if (successor.getHostPort().equals(hostport)) {
				successor.setCompletedexecution(false);
				fillExecutionNotCompletedChildToParent(reversegraph, hostport, successor,
						Graphs.successorListOf(reversegraph, successor));
			}
		}
	}

	/**
	 * Reconfigures stage from the dead to alive executors host and port.
	 * 
	 * @param mdstst
	 * @param pred
	 */
	public void reConfigureParentRDFForStageExecution(StreamPipelineTaskSubmitter mdstst,
			StreamPipelineTaskSubmitter pred) {
		var prdf = mdstst.getTask().parentremotedatafetch;
		for (var rdf : prdf) {
			if (!Objects.isNull(rdf) && rdf.getStageid().equals(pred.getTask().stageid)) {
				rdf.setHp(pred.getHostPort());
				break;
			}
		}
	}

	int containercount;

	/**
	 * Reconfigures stage from the dead to alive executors host and port.
	 * 
	 * @param mstst
	 * @param availablecontainers
	 */
	public void reConfigureContainerForStageExecution(StreamPipelineTaskSubmitter mstst,
			List<String> availablecontainers) {

		var inputs = mstst.getTask().input;
		if (!Objects.isNull(inputs)) {
			for (var input : inputs) {
				if (input instanceof BlocksLocation bsl) {
					bsl.getContainers().retainAll(availablecontainers);
					var containersgrouped = bsl.getContainers().stream()
							.collect(Collectors.groupingBy(key -> key.split(MDCConstants.UNDERSCORE)[0],
									Collectors.mapping(value -> value, Collectors.toCollection(Vector::new))));
					for (var block : bsl.getBlock()) {
						if (!Objects.isNull(block)) {
							var xrefaddrs = block.getDnxref().keySet().stream().map(dnxrefkey -> {
								return block.getDnxref().get(dnxrefkey);
							}).flatMap(xrefaddr -> xrefaddr.stream()).collect(Collectors.toList());

							var containerdnaddr = (List<Tuple2<String, String>>) xrefaddrs.stream()
									.filter(dnxrefkey -> !Objects
											.isNull(containersgrouped.get(dnxrefkey.split(MDCConstants.COLON)[0])))
									.map(dnxrefkey -> {
										List<String> containerstosubmitstage = containersgrouped
												.get(dnxrefkey.split(MDCConstants.COLON)[0]);
										var containerlist = containerstosubmitstage.stream()
												.map(containerhp -> new Tuple2<String, String>(dnxrefkey, containerhp))
												.collect(Collectors.toList());
										return (List<Tuple2<String, String>>) containerlist;
									}).flatMap(containerlist -> containerlist.stream()).collect(Collectors.toList());
							var containerdn = containerdnaddr.get(containercount++ % containerdnaddr.size());
							block.setHp(containerdn.v1);
							bsl.setExecutorhp(containerdn.v2);
							mstst.setHostPort(bsl.getExecutorhp());
							mstst.getTask().hostport = bsl.getExecutorhp();
						}
					}
					mstst.setCompletedexecution(false);
				}
			}
		}
	}


	/**
	 * 
	 * @author arun The task provider for the local mode stage execution.
	 */
	public class TaskProviderLocalMode
			implements TaskProvider<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutorLocal> {

		ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		double totaltasks;
		double counttaskscomp = 0;
		double counttasksfailed = 0;
		Semaphore printresults = new Semaphore(1);

		public TaskProviderLocalMode(double totaltasks) {
			this.totaltasks = totaltasks;
		}

		public com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutorLocal> provideTask(
				final StreamPipelineTaskSubmitter mdstst) {

			return new com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutorLocal>() {
				Task task = mdstst.getTask();
				private static final long serialVersionUID = 1L;

				public StreamPipelineTaskExecutorLocal execute() {
					try (var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());) {
						semaphore.acquire();						
						var mdste = new StreamPipelineTaskExecutorLocal(jsidjsmap.get(task.jobid + task.stageid),
								resultstream, cache);
						mdste.setTask(task);
						mdste.setExecutor(jobping);
						mdste.setHdfs(hdfs);
						Future fut =  es.submit(mdste);
						fut.get();
						Utils.writeToOstream( pipelineconfig.getOutput(),
								"Completed Job And Stages: " + mdstst.getTask().jobid + MDCConstants.HYPHEN
										+ mdstst.getTask().stageid + MDCConstants.HYPHEN
										+ mdstst.getTask().taskid + " in " + mdste.timetaken + " seconds");
						semaphore.release();
						printresults.acquire();
						counttaskscomp++;
						Utils.writeToOstream( pipelineconfig.getOutput(), "\nPercentage Completed "
								+ Math.floor((counttaskscomp / totaltasks) * 100.0) + "% \n");
						printresults.release();
						return mdste;
					} catch (InterruptedException e) {
						log.warn("Interrupted!", e);
						// Restore interrupted state...
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						log.error("SleepyTaskProvider error", e);
					}

					return null;
				}
			};
		}
	}

	/**
	 * 
	 * @author arun The task provider for the ignite mode stage execution.
	 */
	public class TaskProviderIgnite
			implements TaskProvider<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutorIgnite> {

		public com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutorIgnite> provideTask(
				final StreamPipelineTaskSubmitter mdstst) {

			return new com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutorIgnite>() {

				private static final long serialVersionUID = 1L;

				public StreamPipelineTaskExecutorIgnite execute() {
					var task = mdstst.getTask();
					var mdste = new StreamPipelineTaskExecutorIgnite(jsidjsmap.get(task.jobid + task.stageid), task);
					try {
						semaphore.acquire();
						var compute = job.getIgnite().compute(job.getIgnite().cluster().forServers());
						compute.affinityRun(MDCConstants.MDCCACHE, task.input[0], mdste);
						semaphore.release();
					} catch (InterruptedException e) {
						log.warn("Interrupted!", e);
						// Restore interrupted state...
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						log.error("TaskProviderIgnite error", e);
					}
					return mdste;
				}
			};
		}
	}

	/**
	 * 
	 * @author arun The task provider for the standlone mode stage execution.
	 */
	public class DAGScheduler implements TaskProvider<StreamPipelineTaskSubmitter, Boolean> {
		Logger log = Logger.getLogger(DAGScheduler.class);
		double totaltasks;
		double counttaskscomp = 0;
		double counttasksfailed = 0;
		Map<String,Semaphore> semaphores;
		public DAGScheduler(double totaltasks, Map<String,Semaphore> semaphores) {
			this.totaltasks = totaltasks;
			this.semaphores = semaphores;
		}

		Semaphore printresult = new Semaphore(1);

		ConcurrentMap<String, Timer> jobtimer = new ConcurrentHashMap<>();

		public com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, Boolean> provideTask(
				final StreamPipelineTaskSubmitter spts) {

			return new com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, Boolean>() {
				private static final long serialVersionUID = 1L;

				public Boolean execute() {
					
					try {
						semaphores.get(spts.getTask().getHostport()).acquire();
						Boolean result = spts.call();
						printresult.acquire();
						if (Objects.isNull(tetotaltaskscompleted.get(spts.getHostPort()))) {
							tetotaltaskscompleted.put(spts.getHostPort(), 0d);
						}
						counttaskscomp++;
						tetotaltaskscompleted.put(spts.getHostPort(), tetotaltaskscompleted.get(spts.getHostPort()) + 1);
						double percentagecompleted = Math.floor((tetotaltaskscompleted.get(spts.getHostPort()) / servertotaltasks.get(spts.getHostPort())) * 100.0);
						Utils.writeToOstream( pipelineconfig.getOutput(), "\nPercentage Completed TE("
								+ spts.getHostPort() + ") " + percentagecompleted + "% \n");
						job.getJm().getContainersallocated().put(spts.getHostPort(), percentagecompleted);
						printresult.release();
						semaphores.get(spts.getTask().getHostport()).release();
						return result;
					} catch (Exception e) {
					}
					return false;
					
				}
			};
		}

	}

	/**
	 * Calculate the container count via number of processors and container memory
	 * 
	 * @param blocksize
	 * @param stagecount
	 */
	private void decideContainerCountAndPhysicalMemoryByBlockSize(int stagecount, int blocksize) {
		com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory
				.getOperatingSystemMXBean();
		var availablememorysize = os.getFreePhysicalMemorySize();
		var processors = os.getAvailableProcessors();
		blocksize = blocksize * 1024 * 1024;
		availablememorysize = (availablememorysize - blocksize) / processors;
		availablememorysize = availablememorysize / (1024 * 1024);
		System.setProperty("jobcount", "" + stagecount);
		System.setProperty("containercount", "" + processors);
		System.setProperty("containermemory", "" + availablememorysize);
	}

	private Map<String, StreamPipelineTaskSubmitter> taskmdsthread = new ConcurrentHashMap<>();

	/**
	 * Form a graph for physical execution plan for obtaining the topological
	 * ordering of physical execution plan.
	 * 
	 * @param currentstage
	 * @param nextstage
	 * @param stageoutputs
	 * @param jobid
	 * @param graph
	 * @throws Exception
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public void generatePhysicalExecutionPlan(Stage currentstage, Stage nextstage,
			ConcurrentMap<Stage, Object> stageoutputs, String jobid,
			SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph,
			SimpleDirectedGraph<Task, DAGEdge> taskgraph) throws Exception {
		try {
			if (currentstage.tasks.isEmpty()) {
				return;
			}
			var parentstages = new ArrayList<>(currentstage.parent);
			var parent1stage = !parentstages.isEmpty() ? parentstages.get(0) : null;
			List outputparent1 = null;
			if (parent1stage != null) {
				outputparent1 = (List) stageoutputs.get(parent1stage);
			} else {
				outputparent1 = (List) stageoutputs.get(currentstage);
			}
			var parent2stage = parentstages.size() > 1 ? parentstages.get(1) : null;
			List outputparent2 = null;
			if (parent2stage != null) {
				outputparent2 = (List) stageoutputs.get(parent2stage);
			}
			var tasks = new ArrayList<StreamPipelineTaskSubmitter>();
			var function = currentstage.tasks.get(0);
			// Form the graph for intersection function.
			if (function instanceof IntersectionFunction) {
				for (var parentthread1 : outputparent2) {
					for (var parentthread2 : outputparent1) {
						partitionindex++;
						StreamPipelineTaskSubmitter mdstst;
						if ((parentthread1 instanceof BlocksLocation) && (parentthread2 instanceof BlocksLocation)) {
							mdstst = getPipelineTasks(jobid,
									Arrays.asList(parentthread1, parentthread2), currentstage, partitionindex,
									currentstage.number, null);
						} else {
							mdstst = getPipelineTasks(jobid,
									null, currentstage, partitionindex, currentstage.number,
									Arrays.asList(parentthread2, parentthread1));
						}
						taskmdsthread.put(mdstst.getTask().jobid + mdstst.getTask().stageid + mdstst.getTask().taskid,
								mdstst);
						tasks.add(mdstst);
						graph.addVertex(mdstst);
						taskgraph.addVertex(mdstst.getTask());
						if (parentthread1 instanceof StreamPipelineTaskSubmitter mdststparent) {
							if (!graph.containsVertex(mdststparent)) {
								graph.addVertex(mdststparent);
							}
							if (!taskgraph.containsVertex(mdststparent.getTask())) {
								taskgraph.addVertex(mdststparent.getTask());
							}
							graph.addEdge(mdststparent, mdstst);
							taskgraph.addEdge(mdststparent.getTask(), mdstst.getTask());
						}
						if (parentthread2 instanceof StreamPipelineTaskSubmitter mdststparent) {
							if (!graph.containsVertex(mdststparent)) {
								graph.addVertex(mdststparent);
							}
							if (!taskgraph.containsVertex(mdststparent.getTask())) {
								taskgraph.addVertex(mdststparent.getTask());
							}
							graph.addEdge(mdststparent, mdstst);
							taskgraph.addEdge(mdststparent.getTask(), mdstst.getTask());
						}
					}
				}
			}
			// Form the graph for union function.
			else if (function instanceof UnionFunction) {
				if (outputparent1.size() != outputparent2.size()) {
					throw new Exception("Partition Not Equal");
				}
				for (var inputparent1 : outputparent1) {
					for (var inputparent2 : outputparent2) {
						partitionindex++;
						var mdstst = getPipelineTasks(jobid,
								null, currentstage, partitionindex, currentstage.number,
								Arrays.asList(inputparent1, inputparent2));
						taskmdsthread.put(mdstst.getTask().jobid + mdstst.getTask().stageid + mdstst.getTask().taskid,
								mdstst);
						tasks.add(mdstst);
						graph.addVertex(mdstst);
						taskgraph.addVertex(mdstst.getTask());
						if (inputparent1 instanceof StreamPipelineTaskSubmitter input1) {
							if (!graph.containsVertex(input1)) {
								graph.addVertex(input1);
							}
							if (!taskgraph.containsVertex(input1.getTask())) {
								taskgraph.addVertex(input1.getTask());
							}
							graph.addEdge(input1, mdstst);
							taskgraph.addEdge(input1.getTask(), mdstst.getTask());
						}
						if (inputparent2 instanceof StreamPipelineTaskSubmitter input2) {
							if (!graph.containsVertex(input2)) {
								graph.addVertex(input2);
							}
							if (!taskgraph.containsVertex(input2.getTask())) {
								taskgraph.addVertex(input2.getTask());
							}
							graph.addEdge(input2, mdstst);
							taskgraph.addEdge(input2.getTask(), mdstst.getTask());
						}
					}
				}
			}
			// Form the edges and nodes for the JoinPair function.
			else if (function instanceof JoinPredicate || function instanceof LeftOuterJoinPredicate
					|| function instanceof RightOuterJoinPredicate
					|| function instanceof Join
					|| function instanceof LeftJoin
					|| function instanceof RightJoin) {
				for (var inputparent1 : outputparent1) {
					for (var inputparent2 : outputparent2) {
						partitionindex++;
						var mdstst = getPipelineTasks(jobid,
								null, currentstage, partitionindex, currentstage.number,
								Arrays.asList(inputparent1, inputparent2));
						taskmdsthread.put(mdstst.getTask().jobid + mdstst.getTask().stageid + mdstst.getTask().taskid,
								mdstst);
						tasks.add(mdstst);
						graph.addVertex(mdstst);
						taskgraph.addVertex(mdstst.getTask());
						if (inputparent1 instanceof StreamPipelineTaskSubmitter input1) {
							if (!graph.containsVertex(input1)) {
								graph.addVertex(input1);
							}
							if (!taskgraph.containsVertex(input1.getTask())) {
								taskgraph.addVertex(input1.getTask());
							}
							graph.addEdge(input1, mdstst);
							taskgraph.addEdge(input1.getTask(), mdstst.getTask());
						}
						if (inputparent2 instanceof StreamPipelineTaskSubmitter input2) {
							if (!graph.containsVertex(input2)) {
								graph.addVertex(input2);
							}
							if (!taskgraph.containsVertex(input2.getTask())) {
								taskgraph.addVertex(input2.getTask());
							}
							graph.addEdge(input2, mdstst);
							taskgraph.addEdge(input2.getTask(), mdstst.getTask());
						}
					}
				}
			}
			// Form the nodes and edges for aggregate function.
			else if ((function instanceof Coalesce coalesce)) {
				var partkeys = Iterables.partition(outputparent1, (outputparent1.size()) / coalesce.getCoalescepartition())
						.iterator();
				for (; partkeys.hasNext(); ) {
					var parentpartitioned = (List) partkeys.next();
					partitionindex++;
					var mdstst = getPipelineTasks(jobid,
							null, currentstage, partitionindex, currentstage.number, parentpartitioned);
					tasks.add(mdstst);
					graph.addVertex(mdstst);
					taskgraph.addVertex(mdstst.getTask());
					taskmdsthread.put(mdstst.getTask().jobid + mdstst.getTask().stageid + mdstst.getTask().taskid,
							mdstst);
					for (var input : parentpartitioned) {
						var parentthread = (StreamPipelineTaskSubmitter) input;
						if (!graph.containsVertex(parentthread)) {
							graph.addVertex(parentthread);
						}
						if (!taskgraph.containsVertex(parentthread.getTask())) {
							taskgraph.addVertex(parentthread.getTask());
						}
						graph.addEdge(parentthread, mdstst);
						taskgraph.addEdge(parentthread.getTask(), mdstst.getTask());
					}
				}
			} else if (function instanceof AggregateReduceFunction) {
				partitionindex++;
				var mdstst = getPipelineTasks(jobid,
						null, currentstage, partitionindex, currentstage.number, new ArrayList<>(outputparent1));
				tasks.add(mdstst);
				graph.addVertex(mdstst);
				taskgraph.addVertex(mdstst.getTask());
				taskmdsthread.put(mdstst.getTask().jobid + mdstst.getTask().stageid + mdstst.getTask().taskid, mdstst);
				for (var input : outputparent1) {
					var parentthread = (StreamPipelineTaskSubmitter) input;
					if (!graph.containsVertex(parentthread)) {
						graph.addVertex(parentthread);
					}
					if (!taskgraph.containsVertex(parentthread.getTask())) {
						taskgraph.addVertex(parentthread.getTask());
					}
					graph.addEdge(parentthread, mdstst);
					taskgraph.addEdge(parentthread.getTask(), mdstst.getTask());
				}
			} else {
				// Form the nodes and edges for map stage.
				for (var input : outputparent1) {
					partitionindex++;
					StreamPipelineTaskSubmitter mdstst;
					if (input instanceof BlocksLocation) {
						mdstst = getPipelineTasks(jobid,
								input, currentstage, partitionindex, currentstage.number, null);
						taskmdsthread.put(mdstst.getTask().jobid + mdstst.getTask().stageid + mdstst.getTask().taskid,
								mdstst);
						graph.addVertex(mdstst);
						taskgraph.addVertex(mdstst.getTask());
					} else {
						var parentthread = (StreamPipelineTaskSubmitter) input;
						mdstst = getPipelineTasks(jobid,
								null, currentstage, partitionindex, currentstage.number, Arrays.asList(parentthread));
						taskmdsthread.put(mdstst.getTask().jobid + mdstst.getTask().stageid + mdstst.getTask().taskid,
								mdstst);
						graph.addVertex(mdstst);
						taskgraph.addVertex(mdstst.getTask());
						if (!graph.containsVertex(parentthread)) {
							graph.addVertex(parentthread);
						}
						if (!taskgraph.containsVertex(parentthread.getTask())) {
							taskgraph.addVertex(parentthread.getTask());
						}
						graph.addEdge(parentthread, mdstst);
						taskgraph.addEdge(parentthread.getTask(), mdstst.getTask());
					}
					tasks.add(mdstst);
				}
			}
			stageoutputs.put(currentstage, tasks);
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERPHYSICALEXECUTIONPLANERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERPHYSICALEXECUTIONPLANERROR,
					ex);
		}
	}

	/**
	 * Get final stages which has no successors.
	 * 
	 * @param graph
	 * @param mdststs
	 * @return
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public Set<StreamPipelineTaskSubmitter> getFinalPhasesWithNoSuccessors(Graph graph,
			List<StreamPipelineTaskSubmitter> mdststs) {
		return mdststs.stream().filter(mdstst -> Graphs.successorListOf(graph, mdstst).isEmpty())
				.collect(Collectors.toCollection(LinkedHashSet::new));
	}

	/**
	 * Get initial stages which has no predecessors.
	 * 
	 * @param graph
	 * @param mdststs
	 * @return
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public Set<StreamPipelineTaskSubmitter> getFinalPhasesWithNoPredecessors(Graph graph,
			List<StreamPipelineTaskSubmitter> mdststs) {
		return mdststs.stream().filter(mdstst -> Graphs.predecessorListOf(graph, mdstst).isEmpty())
				.collect(Collectors.toCollection(LinkedHashSet::new));
	}

	/**
	 * Obtain the final stage output as when statuses for final stage been
	 * completed.
	 * 
	 * @param graph
	 * @param mdststs
	 * @param ismesos
	 * @param isyarn
	 * @return
	 * @throws PipelineException
	 */
	@SuppressWarnings({"rawtypes"})
	public List getLastStageOutput(Set<StreamPipelineTaskSubmitter> mdstts, Graph graph, List<StreamPipelineTaskSubmitter> mdststs, Boolean ismesos,
			Boolean isyarn, Boolean islocal, Boolean isjgroups,
			ConcurrentMap<String, OutputStream> resultstream) throws PipelineException {
		log.debug("HDFS Path TO Retrieve Final Task Output: " + hdfsfilepath);
		
		try {
			log.debug("Final Stages: " + mdstts);
			Utils.writeToOstream( pipelineconfig.getOutput(), "Final Stages: " + mdstts);

			if (Boolean.TRUE.equals(isignite)) {
				int partition = 0;
				for (var mdstt : mdstts) {
					job.getOutput().add(mdstt);
					if (job.isIsresultrequired()) {
						// Get final stage results from ignite
						writeResultsFromIgnite(mdstt.getTask(), partition++, stageoutput);
					}
				}
			} else if (Boolean.TRUE.equals(islocal)) {
				if (job.getTrigger() != job.getTrigger().SAVERESULTSTOFILE) {
					for (var mdstt : mdstts) {
						var key = getIntermediateResultFS(mdstt.getTask());
						try (var fsstream = resultstream.get(key);
								var input = new FSTObjectInput(
										new ByteBufferInputStream(((ByteBufferOutputStream)fsstream).get()), Utils.getConfigForSerialization());) {
							var obj = input.readObject();
							resultstream.remove(key);
							writeOutputToFile(stageoutput.size(), obj);
							stageoutput.add(obj);
						} catch (Exception ex) {
							log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
							throw ex;
						}
					}
				}
			} else if (Boolean.TRUE.equals(ismesos) || Boolean.TRUE.equals(isyarn)) {
				int partition = 0;
				for (var mdstt : mdstts) {
					// Get final stage results mesos or yarn
					writeOutputToHDFS(hdfs, mdstt.getTask(), partition++, stageoutput);
				}
			} else {
				var ishdfs = false;
				if(nonNull(job.getUri())) {
					ishdfs = new URL(job.getUri()).getProtocol().equals(MDCConstants.HDFS_PROTOCOL);
				}
				for (var mdstt : mdstts) {
					// Get final stage results
					if (mdstt.isCompletedexecution() && job.getTrigger() != job.getTrigger().SAVERESULTSTOFILE || !ishdfs) {
						Task task = mdstt.getTask();
						RemoteDataFetch rdf = new RemoteDataFetch();
						rdf.setHp(task.hostport);
						rdf.setJobid(task.jobid);
						rdf.setStageid(task.stageid);
						rdf.setTaskid(task.taskid);
						boolean isJGroups = Boolean.parseBoolean(pipelineconfig.getJgroups());
						rdf.setMode( isJGroups ? MDCConstants.JGROUPS : MDCConstants.STANDALONE);
						RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
						try (var input = new FSTObjectInput(new ByteArrayInputStream(rdf.getData()), Utils.getConfigForSerialization());) {
							var obj = input.readObject();
							writeOutputToFile(stageoutput.size(), obj);
							stageoutput.add(obj);
						} catch (Exception ex) {
							log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
							throw ex;
						}
					}
				}
			}
			mdstts.clear();
			mdstts = null;
			return stageoutput;
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
		}

	}


	@SuppressWarnings("rawtypes")
	public void writeOutputToFile(int partcount, Object result)
			throws PipelineException, MalformedURLException {
		if (job.getTrigger() == job.getTrigger().SAVERESULTSTOFILE) {
			URL url = new URL(job.getUri());
			boolean isfolder = url.getProtocol().equals(MDCConstants.FILE);
				try (OutputStream fsdos = isfolder?new FileOutputStream(url.getPath() + MDCConstants.FORWARD_SLASH + job.getSavepath() + MDCConstants.HYPHEN + partcount) 
						: hdfs.create(
								new Path(job.getUri().toString() + job.getSavepath() + MDCConstants.HYPHEN + partcount));
						BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsdos))) {
	
					if (result instanceof List res) {
						for (var value : res) {
							bw.write(value.toString());
							bw.write(MDCConstants.NEWLINE);
						}
					}
					else {
						bw.write(result.toString());
					}
					bw.flush();
					fsdos.flush();
				} catch (Exception ioe) {
					log.error(PipelineConstants.FILEIOERROR, ioe);
					throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
				}
			}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void writeOutputToFileInMemory(StreamPipelineTaskSubmitter mdstst, List stageoutput)
			throws PipelineException {
		try (var fsstream = getIntermediateInputStreamInMemory(mdstst.getTask());
					var input = new FSTObjectInput(fsstream, Utils.getConfigForSerialization());) {
			var obj = input.readObject();
			writeOutputToFile(stageoutput.size(), obj);
			stageoutput.add(obj);
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ex);
		}
	}

	/**
	 * Get the file streams from HDFS and jobid and stageid.
	 * 
	 * @param task
	 * @return
	 * @throws Exception
	 */
	private InputStream getIntermediateInputStreamInMemory(Task task) throws Exception {
		try {
			var rdf = new RemoteDataFetch();
			rdf.setJobid(task.jobid);
			rdf.setStageid(task.stageid);
			rdf.setTaskid(task.taskid);
			rdf.setHp(task.hostport);
			boolean isJGroups = Boolean.parseBoolean(pipelineconfig.getJgroups());
			rdf.setMode( isJGroups ? MDCConstants.JGROUPS : MDCConstants.STANDALONE);
			RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
			return new SnappyInputStream(new ByteArrayInputStream(rdf.getData()));
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
		}
	}

	public String getIntermediateDataFSFilePath(String jobid, String stageid, String taskid) {
		return jobid + MDCConstants.HYPHEN + stageid + MDCConstants.HYPHEN + taskid;
	}

	/**
	 * Get the file streams from HDFS and jobid and stageid.
	 * 
	 * @param hdfs
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	private void writeOutputToHDFS(FileSystem hdfs, Task task, int partition, List stageoutput) throws Exception {
		try {
			var path = MDCConstants.FORWARD_SLASH + FileSystemSupport.MDS + MDCConstants.FORWARD_SLASH + task.jobid
					+ MDCConstants.FORWARD_SLASH + task.taskid;
			log.debug("Forming URL Final Stage:" + MDCConstants.FORWARD_SLASH + FileSystemSupport.MDS + MDCConstants.FORWARD_SLASH + task.jobid
					+ MDCConstants.FORWARD_SLASH + task.taskid);
			try (var input = hdfs.open(new Path(path));) {
				byte[] result = input.readAllBytes();
				try(var objectinput = new FSTObjectInput(new ByteArrayInputStream(result), Utils.getConfigForSerialization())){
					stageoutput.add(objectinput.readObject());
				}
			} catch (Exception ex) {
				log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ex);
			}

		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERHDFSDATAFETCHERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERHDFSDATAFETCHERROR, ex);
		}
	}

	/**
	 * Get the file streams from HDFS and jobid and stageid.
	 * @return
	 * @throws Exception
	 */
	private RemoteDataFetch getIntermediateRdfInMemory(Task task) throws Exception {
		try {
			var rdf = new RemoteDataFetch();
			rdf.setJobid(task.jobid);
			rdf.setStageid(task.stageid);
			rdf.setTaskid(task.taskid);
			rdf.setHp(task.hostport);
			boolean isJGroups = Boolean.parseBoolean(pipelineconfig.getJgroups());
			rdf.setMode( isJGroups ? MDCConstants.JGROUPS : MDCConstants.STANDALONE);
			return rdf;
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
		}
	}

	/**
	 * Get intermediate stream obtained from ignite server.
	 * @return InputStream object.
	 * @throws Exception
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	private void writeResultsFromIgnite(Task task, int partition, List stageoutput) throws Exception {
		try {
			log.info("Final Results Ignite Task: " + task);

			try (var sis = 
					new ByteArrayInputStream(job.getIgcache().get(task.jobid + task.stageid + task.taskid));
					var input = new FSTObjectInput(sis, Utils.getConfigForSerialization());) {
				var obj = input.readObject();
				if (!Objects.isNull(job.getUri())) {
					job.setTrigger(job.getTrigger().SAVERESULTSTOFILE);
					writeOutputToFile(partition, obj);
				}
				else {
					stageoutput.add(obj);
				}
			} catch (Exception ex) {
				log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ex);
			}
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
		}
	}

	private String getIntermediateResultFS(Task task) throws Exception {
		return task.jobid + MDCConstants.HYPHEN + task.stageid + MDCConstants.HYPHEN + task.taskid
		;
	}

	private int partitionindex;

	/**
	 * Get the stream thread in order to execute the tasks.
	 * @param jobid
	 * @param input
	 * @param stage
	 * @param partitionindex
	 * @param currentstage
	 * @param parentthreads
	 * @return
	 * @throws PipelineException
	 */
	@SuppressWarnings("rawtypes")
	private StreamPipelineTaskSubmitter getPipelineTasks(String jobid,
														 Object input, Stage stage, int partitionindex, int currentstage, List<Object> parentthreads)
			throws PipelineException {
		try {
			var task = new Task();
			task.jobid = jobid;
			task.stageid = stage.id;
			task.storage = pipelineconfig.getStorage();
			String hp = null;
			task.hbphysicaladdress = hbphysicaladdress;
			if (currentstage == 0 || parentthreads == null) {
				if (input instanceof List inputl) {
					task.input = inputl.toArray();
					hp = ((BlocksLocation) task.input[0]).getExecutorhp();
				} else if (input instanceof BlocksLocation bl) {
					hp = bl.getExecutorhp();
					task.input = new Object[]{input};
				}
				task.parentremotedatafetch = null;

			} else {
				task.input = new Object[parentthreads.size()];
				task.parentremotedatafetch = new RemoteDataFetch[parentthreads.size()];
				for (var parentcount = 0; parentcount < parentthreads.size(); parentcount++) {
					if (parentthreads.get(parentcount) instanceof StreamPipelineTaskSubmitter mdstst) {
						if (!isignite) {
							task.parentremotedatafetch[parentcount] = new RemoteDataFetch();
							task.parentremotedatafetch[parentcount].setJobid(mdstst.getTask().jobid);
							task.parentremotedatafetch[parentcount].setStageid(mdstst.getTask().stageid);
							task.parentremotedatafetch[parentcount].setTaskid(mdstst.getTask().taskid);
							task.parentremotedatafetch[parentcount].setHp(mdstst.getHostPort());
							if (Boolean.parseBoolean(pipelineconfig.getJgroups())) {
								task.parentremotedatafetch[parentcount].setMode(MDCConstants.JGROUPS);
							}
							else {
								task.parentremotedatafetch[parentcount].setMode(MDCConstants.STANDALONE);
							}
							hp = mdstst.getHostPort();
						} else {
							task.input[parentcount] = mdstst.getTask();
						}
					} else if (parentthreads.get(parentcount) instanceof BlocksLocation bl) {
						task.input[parentcount] = bl;
						hp = bl.getExecutorhp();
					} else if (isignite && parentthreads.get(parentcount) instanceof Task insttask) {
						task.input[parentcount] = insttask;
					}
				}
			}
			var mdstst = new StreamPipelineTaskSubmitter(task, hp);
			task.hostport = hp;
			return mdstst;
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERCREATINGSTREAMSCHEDULERTHREAD, ex);
			throw new PipelineException(
					PipelineConstants.JOBSCHEDULERCREATINGSTREAMSCHEDULERTHREAD, ex);
		}
	}

	public void ping(Job job) throws Exception {
		chtssha = TssHAChannel.tsshachannel;
		jobping.execute(() -> {
			while (!istaskcancelled.get()) {
				try (var baos = new ByteArrayOutputStream();
						var lzf = new SnappyOutputStream(baos);
						var output = new FSTObjectOutput(lzf);) {
					job.setPipelineconfig((PipelineConfig) job.getPipelineconfig().clone());
					job.getPipelineconfig().setOutput(null);
					output.writeObject(job);
					chtssha.send(new ObjectMessage(null, baos.toByteArray()));
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					log.warn("Interrupted!", e);
					// Restore interrupted state...
					Thread.currentThread().interrupt();
				} catch (Exception e) {
					log.error(MDCConstants.EMPTY, e);
					break;
				}
			}
		});
	}

	/**
	 * The task executor to execute the job stages in balanced load roundrobin
	 * fashion.
	 * 
	 * @param currentexecutor
	 * @return
	 * @throws Exception
	 */
	public String getTaskExecutorBalanced(long currentexecutor) throws Exception {
		try {
			var executorsl = new ArrayList<String>(taskexecutors);
			return executorsl.get((int) (currentexecutor % executorsl.size()));
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERGETTINGTASKEXECUTORLOADBALANCEDERROR, ex);
			throw new PipelineException(
					PipelineConstants.JOBSCHEDULERGETTINGTASKEXECUTORLOADBALANCEDERROR, ex);
		}
	}
}
