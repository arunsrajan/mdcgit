package com.github.mdc.stream.scheduler;

import java.beans.PropertyChangeListener;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.lang.ref.SoftReference;
import java.net.Socket;
import java.net.URI;
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
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.client.CommandYarnClient;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.dexecutor.core.DefaultDexecutor;
import com.github.dexecutor.core.DexecutorConfig;
import com.github.dexecutor.core.ExecutionConfig;
import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.TaskProvider;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.CloseStagesGraphExecutor;
import com.github.mdc.common.ContainerResources;
import com.github.mdc.common.DAGEdge;
import com.github.mdc.common.DestroyContainer;
import com.github.mdc.common.DestroyContainers;
import com.github.mdc.common.FileSystemSupport;
import com.github.mdc.common.FreeResourcesCompletedJob;
import com.github.mdc.common.GlobalContainerAllocDealloc;
import com.github.mdc.common.HeartBeatServerStream;
import com.github.mdc.common.HeartBeatTaskSchedulerStream;
import com.github.mdc.common.Job;
import com.github.mdc.common.JobApp;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.KryoPool;
import com.github.mdc.common.LoadJar;
import com.github.mdc.common.MDCCache;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCNodesResourcesSnapshot;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.MassiveDataPipelineConstants;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.RemoteDataFetch;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.Stage;
import com.github.mdc.common.Task;
import com.github.mdc.common.TasksGraphExecutor;
import com.github.mdc.common.TssHAChannel;
import com.github.mdc.common.TssHAHostPorts;
import com.github.mdc.common.Utils;
import com.github.mdc.common.WhoIsResponse;
import com.github.mdc.stream.PipelineException;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutor;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorLocal;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorIgnite;
import com.github.mdc.stream.functions.AggregateReduceFunction;
import com.github.mdc.stream.functions.Coalesce;
import com.github.mdc.stream.functions.IntersectionFunction;
import com.github.mdc.stream.functions.JoinPredicate;
import com.github.mdc.stream.functions.LeftOuterJoinPredicate;
import com.github.mdc.stream.functions.RightOuterJoinPredicate;
import com.github.mdc.stream.functions.UnionFunction;
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
	Cache cache = null;
	public Semaphore semaphore;
	public HeartBeatTaskSchedulerStream hbtss;
	public HeartBeatServerStream hbss;
	private Kryo kryo = Utils.getKryoNonDeflateSerializer();
	public PipelineConfig pipelineconfig;
	AtomicBoolean istaskcancelled = new AtomicBoolean();
	public Map<String, JobStage> jsidjsmap = new ConcurrentHashMap<>();
	public List<Object> stageoutput = new ArrayList<>();
	String hdfsfilepath = null;
	FileSystem hdfs = null;
	public StreamJobScheduler() {
		hdfsfilepath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL);
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
	@SuppressWarnings({ "unchecked", "rawtypes", "resource" })
	public Object schedule(Job job) throws Exception {
		this.job = job;
		this.pipelineconfig = job.pipelineconfig;
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

		try (var hbtss = new HeartBeatTaskSchedulerStream(); 
				var hbss = new HeartBeatServerStream();
				var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());) {
			this.hdfs = hdfs;
			this.hbtss = hbtss;
			this.hbss = hbss;
			// If not yarn and mesos start the resources and task scheduler
			// heart beats
			// for standalone MDC task schedulers and executors to communicate
			// via task statuses.
			if (Boolean.FALSE.equals(ismesos) && Boolean.FALSE.equals(isyarn) && Boolean.FALSE.equals(islocal)
					&& Boolean.FALSE.equals(isjgroups) && !isignite) {
				// Initialize the heart beat for gathering the resources
				// Initialize the heart beat for gathering the task executors
				// task statuses information.
				hbtss.init(Integer.parseInt(pipelineconfig.getRescheduledelay()),
						Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT)),
						MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST),
						Integer.parseInt(pipelineconfig.getInitialdelay()),
						Integer.parseInt(pipelineconfig.getPingdelay()), MDCConstants.EMPTY, job.id);
				// Start the heart beat to receive task executor to task
				// schedulers task status updates.
				hbtss.start();
				hbtss.getHbo().start();
				getContainersHostPort();
				batchsize = Integer.parseInt(pipelineconfig.getBatchsize());
				taskexecutors.parallelStream().collect(Collectors.toMap(val -> val, val -> new Semaphore(batchsize)));
				var taskexecutorssize = taskexecutors.size();
				taskexecutorssize = taskexecutors.size();
				log.debug("taskexecutorssize: " + taskexecutorssize);
				Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(), "taskexecutorssize: " + taskexecutorssize);
				boolean haenabled = Boolean.parseBoolean(pipelineconfig.getTsshaenabled());
				if (!Objects.isNull(pipelineconfig.getJar()) && haenabled) {
					TssHAHostPorts.get().forEach(hp -> {
						try {
							LoadJar lj = new LoadJar();
							lj.mrjar = pipelineconfig.getJar();
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
			var uniquestagestoprocess = new ArrayList<>(job.topostages);
			var stagenumber = 0;
			var graph = new SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge>(DAGEdge.class);
			var taskgraph = new SimpleDirectedGraph<Task, DAGEdge>(DAGEdge.class);
			// Generate Physical execution plan for each stages.
			if (Objects.isNull(job.vertices)) {
				for (var stage : uniquestagestoprocess) {
					JobStage js = new JobStage();
					js.jobid = job.id;
					js.stageid = stage.id;
					js.stage = stage;
					jsidjsmap.put(job.id + stage.id, js);
					partitionindex = 0;
					var nextstage = stagenumber + 1 < uniquestagestoprocess.size()
							? uniquestagestoprocess.get(stagenumber + 1)
							: null;
					stage.number = stagenumber;
					generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
					stagenumber++;
				}
				job.vertices = new LinkedHashSet<>(graph.vertexSet());
				job.edges = new LinkedHashSet<>(graph.edgeSet());
			} else {
				job.vertices.stream().forEach(vertex -> graph.addVertex((StreamPipelineTaskSubmitter) vertex));
				job.edges.stream()
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
			while (topostages.hasNext())
				mdststs.add(topostages.next());
			log.debug(mdststs);
			var mdstts = getFinalPhasesWithNoSuccessors(graph, mdststs);
			var partitionnumber = 0;
			for (var mdstst : mdstts) {
				mdstst.getTask().finalphase = true;
				if (job.trigger == Job.TRIGGER.SAVERESULTSTOFILE
						&& (mdstst.getTask().storage == MDCConstants.STORAGE.INMEMORY
						|| mdstst.getTask().storage == MDCConstants.STORAGE.INMEMORY_DISK) ) {
					mdstst.getTask().saveresulttohdfs = true;
					mdstst.getTask().hdfsurl = job.uri;
					mdstst.getTask().filepath = job.savepath + MDCConstants.HYPHEN + partitionnumber++;
				}
			}
			Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(), "stages: " + mdststs);
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
				var yarninputfolder = MDCConstants.YARNINPUTFOLDER + MDCConstants.BACKWARD_SLASH + job.id;
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
						MDCConstants.BACKWARD_SLASH + YarnSystemConstants.DEFAULT_CONTEXT_FILE_CLIENT, getClass());
				var client = (CommandYarnClient) context.getBean(MDCConstants.YARN_CLIENT);
				client.getEnvironment().put(MDCConstants.YARNMDCJOBID, job.id);
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
				var tasksgraphexecutor = new TasksGraphExecutor[taskexecutors.size()];
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
						var hp = stagesgraphexecutor.getHostport().split(MDCConstants.UNDERSCORE);
						var client = new Socket(hp[0], Integer.parseInt(hp[1]));
						var kryo = Utils.getKryoNonDeflateSerializer();
						var output = new Output(client.getOutputStream());
						kryo.writeClassAndObject(output, stagesgraphexecutor);
						output.flush();
						client.close();
					}
				}
				var stagepartids = tasks.parallelStream().map(taskpart -> taskpart.taskid).collect(Collectors.toSet());
				var stagepartidstatusmapresp = new ConcurrentHashMap<String, WhoIsResponse.STATUS>();
				var stagepartidstatusmapreq = new ConcurrentHashMap<String, WhoIsResponse.STATUS>();
				try (var channel = Utils.getChannelTaskExecutor(job.id,
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
						if (totalcompleted == totaltasks) {
							Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(), "\nPercentage Completed "
									+ Math.floor((totalcompleted / totaltasks) * 100.0) + "% \n");
							break;
						} else {
							log.debug(
									"Total Percentage Completed: " + Math.floor((totalcompleted / totaltasks) * 100.0));
							Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(), "\nPercentage Completed "
									+ Math.floor((totalcompleted / totaltasks) * 100.0) + "% \n");
							Thread.sleep(4000);
						}
					}
					stagepartidstatusmapresp.clear();
					stagepartidstatusmapreq.clear();
					closeResourcesTaskExecutor(tasksgraphexecutor);
				} catch (Exception e) {
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
			job.iscompleted = true;
			job.jm.jobcompletiontime = System.currentTimeMillis();
			Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(),
					"Completed Job in " + ((job.jm.jobcompletiontime - job.jm.jobstarttime) / 1000.0) + " seconds");
			log.info("Completed Job in " + ((job.jm.jobcompletiontime - job.jm.jobstarttime) / 1000.0) + " seconds");
			job.jm.totaltimetaken = (job.jm.jobcompletiontime - job.jm.jobstarttime) / 1000.0;
			Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(),
					"Job Metrics " + job.jm);
			log.info("Job Metrics " + job.jm);
			if (Boolean.TRUE.equals(islocal)) {
				var srresultstore = new SoftReference<ConcurrentMap<String, OutputStream>>(resultstream);
				srresultstore.clear();
				resultstream = null;
			}
			return finalstageoutput;
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.JOBSCHEDULERERROR, ex);
			throw new PipelineException(MassiveDataPipelineConstants.JOBSCHEDULERERROR, ex);
		} finally {
			if (!Objects.isNull(job.igcache)) {
				job.igcache.close();
			}
			if (!Objects.isNull(job.ignite)) {
				job.ignite.close();
			}
			if ((Boolean.FALSE.equals(ismesos) && Boolean.FALSE.equals(isyarn) && Boolean.FALSE.equals(islocal)
					|| Boolean.TRUE.equals(isjgroups)) && !isignite) {
				if (!Boolean.TRUE.equals(isjgroups)) {
					var cce = new FreeResourcesCompletedJob();
					cce.jobid = job.id;
					cce.containerid = job.containerid;
					for (var te : taskexecutors) {
						Utils.writeObject(te, cce);
					}
				}
				destroyContainers();
			}
			if (!Objects.isNull(hbss)) {
				hbss.close();
			}
			if (!Objects.isNull(hbtss)) {
				hbtss.close();
			}
			if (!Objects.isNull(job.allstageshostport)) {
				job.allstageshostport.clear();
			}
			if (!Objects.isNull(job.stageoutputmap)) {
				job.stageoutputmap.clear();
			}
			istaskcancelled.set(true);
			jobping.shutdownNow();
		}
	}

	public void broadcastJobStageToTaskExecutors() throws Exception {
		jsidjsmap.keySet().stream().forEach(key -> {
			for (String te : taskexecutors) {
				try {
					JobStage js = jsidjsmap.get(key);
					Utils.writeObject(te, js);
				} catch (Exception e) {
					log.error(MDCConstants.EMPTY, e);
				}
			}
		});
	}

	/**
	 * Get containers host port by launching.
	 * 
	 * @param hbss
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public void getContainersHostPort() throws PipelineException {
		try {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().acquire();
			var loadjar = new LoadJar();
			loadjar.mrjar = pipelineconfig.getJar();
			for (var lc : job.lcs) {
				List<Integer> ports = (List<Integer>) Utils.getResultObjectByInput(lc.getNodehostport(), lc);
				int index = 0;
				String tehost = lc.getNodehostport().split("_")[0];
				while (index < ports.size()) {
					while (true) {
						try (var sock = new Socket(tehost, ports.get(index));) {
							if (!Objects.isNull(loadjar.mrjar)) {
								Utils.writeObject(sock, loadjar);
							}
							break;
						} catch (Exception ex) {
							Thread.sleep(1000);
						}
					}
					JobApp jobapp = new JobApp();
					jobapp.setContainerid(lc.getContainerid());
					jobapp.setJobappid(job.id);
					jobapp.setJobtype(JobApp.JOBAPP.STREAM);
					Utils.writeObject(tehost + MDCConstants.UNDERSCORE + ports.get(index), jobapp);
					index++;
				}
			}
			hbss.init(
					Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY)),
					Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT)),
					NetworkUtil
							.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST)),
					Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_INITIALDELAY)),
					Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PINGDELAY)),
					job.containerid);
			// Start Resources gathering via heart beat resources status update.
			hbss.start();
			taskexecutors = new LinkedHashSet<>(hbss.containers);
			while (taskexecutors.size() != job.containers.size()) {
				taskexecutors = new LinkedHashSet<>(hbss.containers);
				Thread.sleep(500);
			}
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.JOBSCHEDULERCONTAINERERROR, ex);
			throw new PipelineException(MassiveDataPipelineConstants.JOBSCHEDULERCONTAINERERROR, ex);
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
			if (!Objects.isNull(job.nodes)) {
				var nodes = job.nodes;
				var contcontainerids = GlobalContainerAllocDealloc.getContainercontainerids();
				var chpcres = GlobalContainerAllocDealloc.getHportcrs();
				var deallocateall = true;
				if (!Objects.isNull(job.containers)) {
					for (String container : job.containers) {
						var cids = contcontainerids.get(container);
						cids.remove(job.containerid);
						if (cids.isEmpty()) {
							contcontainerids.remove(container);
							var dc = new DestroyContainer();
							dc.setContainerid(job.containerid);
							dc.setContainerhp(container);
							String node = GlobalContainerAllocDealloc.getContainernode().remove(container);
							Set<String> containers = GlobalContainerAllocDealloc.getNodecontainers().get(node);
							containers.remove(container);
							Utils.writeObject(node, dc);
							ContainerResources cr = chpcres.remove(container);
							long freememory = MDCNodesResourcesSnapshot.get().get(node).getFreememory();
							long cpu = MDCNodesResourcesSnapshot.get().get(node).getNumberofprocessors();
							MDCNodesResourcesSnapshot.get().get(node).setFreememory(freememory + cr.getMaxmemory()*MDCConstants.MB);
							MDCNodesResourcesSnapshot.get().get(node).setNumberofprocessors((int) (cpu + cr.getCpu()));
						} else {
							deallocateall = false;
						}
					}
				}
				if (deallocateall) {
					var dc = new DestroyContainers();
					dc.setContainerid(job.containerid);
					log.debug("Destroying Containers with id:" + job.containerid + " for the hosts: " + nodes);
					for (var node : nodes) {
						long freememory = MDCNodesResourcesSnapshot.get().get(node).getFreememory();
						MDCNodesResourcesSnapshot.get().get(node).setFreememory(freememory + 256 * MDCConstants.MB);
						int cpu = MDCNodesResourcesSnapshot.get().get(node).getNumberofprocessors();
						MDCNodesResourcesSnapshot.get().get(node).setNumberofprocessors((int) (cpu + 2));
						Utils.writeObject(node, dc);
					}
				}
			}
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.DESTROYCONTAINERERROR, ex);
			throw new PipelineException(MassiveDataPipelineConstants.DESTROYCONTAINERERROR, ex);
		} finally {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().release();
		}
	}

	/**
	 * Closes Stages in task executor making room for another tasks to be executed
	 * in tkas executors JVM.
	 * 
	 * @param tasksgraphexecutor
	 * @throws Exception
	 */
	private void closeResourcesTaskExecutor(TasksGraphExecutor[] tasksgraphexecutor) throws Exception {
		for (var taskgraphexecutor : tasksgraphexecutor) {
			if (!taskgraphexecutor.getTasks().isEmpty()) {
				var hp = taskgraphexecutor.getHostport();
				Utils.writeObject(hp, new CloseStagesGraphExecutor(taskgraphexecutor.getTasks().get(0).jobid));
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
	@SuppressWarnings({ "unchecked", "rawtypes" })
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
			log.error(MassiveDataPipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
			throw new PipelineException(MassiveDataPipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
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
	@SuppressWarnings({ "unchecked", "rawtypes" })
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
			log.error(MassiveDataPipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
			throw new PipelineException(MassiveDataPipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
		} finally {
			if (!Objects.isNull(es)) {
				es.shutdownNow();
				es.awaitTermination(1, TimeUnit.SECONDS);
			}
		}
	}

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
			es = newExecutor(batchsize);
			List<ExecutionResult<StreamPipelineTaskSubmitter, Boolean>> erroredresult = null;			

			var temdstdtmap = new ConcurrentHashMap<String, List<StreamPipelineTaskSubmitter>>();			

			while (!completed && numexecute < executioncount) {
				temdstdtmap.clear();;
				var configexec = new DexecutorConfig<StreamPipelineTaskSubmitter, Boolean>(es,
						new DAGScheduler(graph.vertexSet().size()));
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
						mdststs = temdstdtmap.get(mdstst.getHostPort());
						if (Objects.isNull(mdststs)) {
							mdststs = new ArrayList<>();
							temdstdtmap.put(mdstst.getHostPort(), mdststs);
						}
						mdststs.add(mdstst);
					
					}
				}
				var config = new DexecutorConfig<DefaultDexecutor<StreamPipelineTaskSubmitter, Boolean>, List<ExecutionResult<StreamPipelineTaskSubmitter, Boolean>>>(
						newExecutor(temdstdtmap.keySet().size()), new DAGSchedulerInitialStage());
				var initialstageexecutor = new DefaultDexecutor<DefaultDexecutor<StreamPipelineTaskSubmitter, Boolean>, List<ExecutionResult<StreamPipelineTaskSubmitter, Boolean>>>(
						config);
				temdstdtmap.keySet().stream().forEach(key -> {
					var mdststsl = temdstdtmap.get(key);
					var configinitialstage = new DexecutorConfig<StreamPipelineTaskSubmitter, Boolean>(
							newExecutor(batchsize), new DAGScheduler(mdststsl.size()));
					var dexecutorinitialstage = new DefaultDexecutor<StreamPipelineTaskSubmitter, Boolean>(
							configinitialstage);
					initialstageexecutor.addIndependent(dexecutorinitialstage);
					mdststsl.stream().forEach(mdststl -> {
						dexecutorinitialstage.addDependency(mdststl,mdststl);
					});
				});
				var executionresults = initialstageexecutor.execute(ExecutionConfig.NON_TERMINATING);
				var erroredresultinitialstage = executionresults.getErrored();
				if (!erroredresultinitialstage.isEmpty()) {
					numexecute++;
					completed = false;
					continue;
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
				hbtss.clearStageJobStageMap();
				hbtss.getHbo().clearQueue();
				hbtss.getHbo().removePropertyChangeListeners();
			}
			Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(), "Number of Executions: " + numexecute);
			if (!completed) {
				StringBuilder sb = new StringBuilder();
				erroredresult.forEach(exec -> {
					sb.append(MDCConstants.NEWLINE);
					sb.append(exec.getId().getTask().stagefailuremessage);
				});
				if(!sb.isEmpty()) {
					throw new PipelineException(MassiveDataPipelineConstants.ERROREDTASKS.replace("%s", ""+executioncount)+sb.toString());
				}
			}
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
			throw new PipelineException(MassiveDataPipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
		} finally {
			if (!Objects.isNull(es)) {
				es.shutdownNow();
				es.awaitTermination(1, TimeUnit.SECONDS);
			}
		}
	}
	
	
	private class DAGSchedulerInitialStage implements
			TaskProvider<DefaultDexecutor<StreamPipelineTaskSubmitter, Boolean>, List<ExecutionResult<StreamPipelineTaskSubmitter, Boolean>>> {

		@Override
		public com.github.dexecutor.core.task.Task<DefaultDexecutor<StreamPipelineTaskSubmitter, Boolean>, List<ExecutionResult<StreamPipelineTaskSubmitter, Boolean>>> provideTask(
				DefaultDexecutor<StreamPipelineTaskSubmitter, Boolean> dexecutor) {
			return new com.github.dexecutor.core.task.Task<DefaultDexecutor<StreamPipelineTaskSubmitter, Boolean>, List<ExecutionResult<StreamPipelineTaskSubmitter, Boolean>>>() {

				private static final long serialVersionUID = 1L;

				@Override
				public List<ExecutionResult<StreamPipelineTaskSubmitter, Boolean>> execute() {
					var execresults = dexecutor.execute(ExecutionConfig.NON_TERMINATING);
					return execresults.getErrored();
				}
			};
		}

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
		for (; toposort.hasNext();) {
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
			if (!Objects.isNull(rdf) && rdf.stageid.equals(pred.getTask().stageid)) {
				rdf.hp = pred.getHostPort();
				break;
			}
		}
	}

	int containercount = 0;

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
					bsl.containers.retainAll(availablecontainers);
					var containersgrouped = bsl.containers.stream()
							.collect(Collectors.groupingBy(key -> key.split(MDCConstants.UNDERSCORE)[0],
									Collectors.mapping(value -> value, Collectors.toCollection(Vector::new))));
					for (var block : bsl.block) {
						if (!Objects.isNull(block)) {
							var xrefaddrs = block.dnxref.keySet().stream().map(dnxrefkey -> {
								return block.dnxref.get(dnxrefkey);
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
							block.hp = containerdn.v1;
							bsl.executorhp = containerdn.v2;
							mstst.setHostPort(bsl.executorhp);
							mstst.getTask().hostport = bsl.executorhp;
						}
					}
					mstst.setCompletedexecution(false);
				}
			}
		}
	}

	/**
	 * Creates Executors
	 * 
	 * @return ExecutorsService object.
	 */
	private ExecutorService newExecutor(int numberoftasks) {
		return Executors.newWorkStealingPool();
	}

	/**
	 * 
	 * @author arun The task provider for the local mode stage execution.
	 */
	public class TaskProviderLocalMode
			implements TaskProvider<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutorLocal> {

		
		double totaltasks;
		double counttaskscomp=0;
		double counttasksfailed=0;
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
					var mdste = new StreamPipelineTaskExecutorLocal(jsidjsmap.get(task.jobid + task.stageid),
							resultstream, cache);
					mdste.setTask(task);
					mdste.setExecutor(jobping);
					mdste.setHdfs(hdfs);
					try {
						semaphore.acquire();
						mdste.call();
						Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(),
								"Completed Job And Stages: " + mdstst.getTask().jobid + MDCConstants.HYPHEN
										+ mdstst.getTask().stageid + " in " + mdste.timetaken + " seconds");
						semaphore.release();
						printresults.acquire();
						counttaskscomp++;
						Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(), "\nPercentage Completed "
								+ Math.floor((counttaskscomp / totaltasks) * 100.0) + "% \n");
						printresults.release();						
					} catch (Exception e) {
						log.error("SleepyTaskProvider error", e);
					}

					return mdste;
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
						var compute = job.ignite.compute(job.ignite.cluster().forServers());
						compute.affinityRun(MDCConstants.MDCCACHE, task.input[0], mdste);
						Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(),
								"Completed Job And Stages: " + task.jobid + MDCConstants.HYPHEN + task.stageid);
						Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(), "Completed Tasks: " + task);
						semaphore.release();
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
		double counttaskscomp=0;
		double counttasksfailed=0;
		public DAGScheduler(double totaltasks) {
			this.totaltasks = totaltasks;
		}
		
		Semaphore printresult = new Semaphore(1);
		
		Semaphore mutex = new Semaphore(batchsize);
		ConcurrentMap<String, Timer> jobtimer = new ConcurrentHashMap<>();

		public com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, Boolean> provideTask(
				final StreamPipelineTaskSubmitter mdstst) {

			return new com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, Boolean>() {
				final StreamPipelineTaskSubmitter mdststlocal = mdstst;
				private static final long serialVersionUID = 1L;

				public Boolean execute() {					
					if (mdststlocal.isCompletedexecution()) {
						try {
							printresult.acquire();
							counttaskscomp++;
							if (job.trigger != Job.TRIGGER.SAVERESULTSTOFILE && !mdststlocal.isResultobtainedte() 
									&& mdststlocal.getTask().finalphase && mdststlocal.isCompletedexecution() 
									&& (mdststlocal.getTask().storage == MDCConstants.STORAGE.INMEMORY
									||mdststlocal.getTask().storage == MDCConstants.STORAGE.INMEMORY_DISK)) {
								writeOutputToFileInMemory(mdststlocal,kryo,stageoutput);
							}
							double percentagecompleted = Math.floor((counttaskscomp / totaltasks) * 100.0);
							Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(), "\nPercentage Completed TE("
									+ mdststlocal.getHostPort() + ") " + percentagecompleted + "% \n");
							job.jm.containersallocated.put(mdststlocal.getHostPort(), percentagecompleted);
							printresult.release();
						} catch (Exception e) {
							log.info("DAGTaskExecutor error", e);
						}
						return true;
					}
					var cdl = new CountDownLatch(1);
					PropertyChangeListener observer = (evt) -> {

						try {
							final var task = (Task) evt.getNewValue();
							if (mdststlocal.getTask().taskid.equals(task.taskid)
									&& task.taskstatus == Task.TaskStatus.COMPLETED) {
								var timer = jobtimer.get(task.taskid);
								if (!Objects.isNull(timer)) {
									timer.cancel();
									timer.purge();
								}
								jobtimer.remove(task.taskid);
								mdststlocal.setCompletedexecution(true);
								printresult.acquire();
								counttaskscomp++;
								if (job.trigger != Job.TRIGGER.SAVERESULTSTOFILE && mdststlocal.getTask().finalphase 
										&& mdststlocal.isCompletedexecution() 
										&& (mdststlocal.getTask().storage == MDCConstants.STORAGE.INMEMORY
										||mdststlocal.getTask().storage == MDCConstants.STORAGE.INMEMORY_DISK)) {
									writeOutputToFileInMemory(mdststlocal,kryo,stageoutput);
									mdststlocal.setResultobtainedte(true);
								}
								double percentagecompleted = Math.floor((counttaskscomp / totaltasks) * 100.0);
								Object[] input=mdststlocal.getTask().input;
								Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(), "Task Completed ("+(Objects.isNull(input)?task:input[0])+") "
										+ task.timetakenseconds + " seconds\n");
								log.info("Task Completed ("+(Objects.isNull(input)?task:input[0])+") "
										+ task.timetakenseconds + " seconds\n");
								Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(), "\nPercentage Completed TE("+mdststlocal.getHostPort()+") "
										+ percentagecompleted + "% \n");
								log.info("\nPercentage Completed TE("+mdststlocal.getHostPort()+") "
										+ percentagecompleted + "% \n");
								job.jm.containersallocated.put(mdststlocal.getHostPort(),percentagecompleted);								
								printresult.release();
								cdl.countDown();
								
							} else if (mdststlocal.getTask().taskid.equals(task.taskid)
									&& task.taskstatus == Task.TaskStatus.FAILED) {
								var timer = jobtimer.get(task.taskid);
								if (!Objects.isNull(timer)) {
									timer.cancel();
									timer.purge();
								}
								jobtimer.remove(task.taskid);
								mdststlocal.setCompletedexecution(false);
								mdststlocal.getTask().stagefailuremessage = task.stagefailuremessage;
								printresult.acquire();
								counttasksfailed++;
								Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(), "\nPercentage Failed TE("+mdststlocal.getHostPort()+") "
										+ Math.floor((counttasksfailed / totaltasks) * 100.0) + "% \n");
								log.info("\nPercentage Failed TE("+mdststlocal.getHostPort()+") "
										+ Math.floor((counttasksfailed / totaltasks) * 100.0) + "% \n");
								printresult.release();
								cdl.countDown();
							}
						} catch (Exception e) {
							log.info("DAGTaskExecutor error", e);
						} finally {

						}
					};

					try {
						mutex.acquire();
						hbtss.getHbo().addPropertyChangeListener(observer);
						if (taskexecutors.contains(mdststlocal.getHostPort())) {
							var timer = new Timer();
							jobtimer.put(mdststlocal.getTask().taskid, timer);
							var delay = Long.parseLong(pipelineconfig.getInitialdelay());
							timer.scheduleAtFixedRate(new TimerTask() {
								int count = 0;

								@Override
								public void run() {
									try {
										if (++count > 10 || !taskexecutors.contains(mdststlocal.getHostPort())) {
											mdststlocal.setCompletedexecution(false);
											cdl.countDown();
											var timer = jobtimer.remove(mdststlocal.getTask().taskid);
											if (!Objects.isNull(timer)) {
												timer.cancel();
												timer.purge();
											}
											hbtss.pingOnce(mdststlocal.getTask().stageid, mdststlocal.getTask().taskid,
													mdststlocal.getHostPort(), Task.TaskStatus.FAILED, 0.0d,
													MDCConstants.TSEXCEEDEDEXECUTIONCOUNT);
										}
									} catch (Exception ex) {
										log.error(MDCConstants.EMPTY, ex);
									}

								}
							}, delay, delay);
							Utils.writeObject(mdststlocal.getHostPort(), mdststlocal.getTask());
							cdl.await();
						} else {
							mdststlocal.setCompletedexecution(false);
						}
						mutex.release();
						hbtss.getHbo().removePropertyChangeListener(observer);
					} catch (Exception e) {
						log.info("Job Submission error", e);
						mdststlocal.setCompletedexecution(false);
					}
					if (!mdststlocal.isCompletedexecution()) {
						throw new IllegalArgumentException("Incomplete task");
					}
					return mdststlocal.isCompletedexecution();
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
	 * @param allstageshp
	 * @param jobid
	 * @param graph
	 * @throws Exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
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
							mdstst = getMassiveDataStreamTaskSchedulerThread(jobid,
									currentstage.id + MDCConstants.HYPHEN + currentstage.number + MDCConstants.HYPHEN
											+ partitionindex,
									Arrays.asList(parentthread1, parentthread2), currentstage, partitionindex,
									currentstage.number, null);
						} else {
							mdstst = getMassiveDataStreamTaskSchedulerThread(jobid,
									currentstage.id + MDCConstants.HYPHEN + currentstage.number + MDCConstants.HYPHEN
											+ partitionindex,
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
						var mdstst = getMassiveDataStreamTaskSchedulerThread(jobid,
								currentstage.id + MDCConstants.HYPHEN + currentstage.number + MDCConstants.HYPHEN
										+ partitionindex,
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
					|| function instanceof RightOuterJoinPredicate) {
				for (var inputparent1 : outputparent1) {
					for (var inputparent2 : outputparent2) {
						partitionindex++;
						var mdstst = getMassiveDataStreamTaskSchedulerThread(jobid,
								currentstage.id + MDCConstants.HYPHEN + currentstage.number + MDCConstants.HYPHEN
										+ partitionindex,
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
				var partkeys = Iterables.partition(outputparent1, (outputparent1.size()) / coalesce.coalescepartition)
						.iterator();
				for (; partkeys.hasNext();) {
					var parentpartitioned = (List) partkeys.next();
					partitionindex++;
					var mdstst = getMassiveDataStreamTaskSchedulerThread(jobid,
							currentstage.id + MDCConstants.HYPHEN + currentstage.number + MDCConstants.HYPHEN
									+ partitionindex,
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
				var mdstst = getMassiveDataStreamTaskSchedulerThread(jobid,
						currentstage.id + MDCConstants.HYPHEN + currentstage.number + MDCConstants.HYPHEN
								+ partitionindex,
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
						mdstst = getMassiveDataStreamTaskSchedulerThread(jobid,
								currentstage.id + MDCConstants.HYPHEN + currentstage.number + MDCConstants.HYPHEN
										+ partitionindex,
								input, currentstage, partitionindex, currentstage.number, null);
						taskmdsthread.put(mdstst.getTask().jobid + mdstst.getTask().stageid + mdstst.getTask().taskid,
								mdstst);
						graph.addVertex(mdstst);
						taskgraph.addVertex(mdstst.getTask());
					} else {
						var parentthread = (StreamPipelineTaskSubmitter) input;
						mdstst = getMassiveDataStreamTaskSchedulerThread(jobid,
								currentstage.id + MDCConstants.HYPHEN + currentstage.number + MDCConstants.HYPHEN
										+ partitionindex,
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
			log.error(MassiveDataPipelineConstants.JOBSCHEDULERPHYSICALEXECUTIONPLANERROR, ex);
			throw new PipelineException(MassiveDataPipelineConstants.JOBSCHEDULERPHYSICALEXECUTIONPLANERROR,
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
	@SuppressWarnings({ "unchecked", "rawtypes" })
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
	@SuppressWarnings({ "unchecked", "rawtypes" })
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
	@SuppressWarnings({ "rawtypes" })
	public List getLastStageOutput(Set<StreamPipelineTaskSubmitter> mdstts,Graph graph, List<StreamPipelineTaskSubmitter> mdststs, Boolean ismesos,
			Boolean isyarn, Boolean islocal, Boolean isjgroups,
			ConcurrentMap<String, OutputStream> resultstream) throws PipelineException {		
		log.debug("HDFS Path TO Retrieve Final Task Output: " + hdfsfilepath);
		var configuration = new Configuration();
		var kryofinal = KryoPool.getKryoPool().obtain();
		try (var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), configuration);) {
			log.debug("Final Stages: " + mdstts);
			Utils.writeKryoOutput(kryo, pipelineconfig.getOutput(), "Final Stages: " + mdstts);
			
			if (Boolean.TRUE.equals(isignite)) {
				int partition = 0;
				for (var mdstt : mdstts) {
					job.output.add(mdstt);
					if (job.isresultrequired) {
						// Get final stage results from ignite
						writeResultsFromIgnite(hdfs, mdstt.getTask(), partition++, stageoutput);
					}
				}
			} else if (Boolean.TRUE.equals(islocal)) {
				if(job.trigger != Job.TRIGGER.SAVERESULTSTOFILE) {
					for (var mdstt : mdstts) {
						var key = getIntermediateResultFS(mdstt.getTask());
						try (var fsstream = resultstream.get(key);
								var input = new Input(
										new SnappyInputStream(new ByteArrayInputStream(((ByteArrayOutputStream)fsstream).toByteArray())));) {
							var obj = kryofinal.readClassAndObject(input);
							resultstream.remove(key);
							writeOutputToFile(stageoutput.size(),obj);
							stageoutput.add(obj);
						} catch (Exception ex) {
							log.error(MassiveDataPipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
							throw ex;
						}
					}
				}
			} else if (Boolean.TRUE.equals(ismesos) || Boolean.TRUE.equals(isyarn) || Boolean.TRUE.equals(isjgroups)
					&& job.trigger != Job.TRIGGER.SAVERESULTSTOFILE) {
				int partition = 0;
				for (var mdstt : mdstts) {
					// Get final stage results mesos or yarn
					writeOutputToHDFS(hdfs,mdstt.getTask(),partition++,stageoutput);
				}
			} else {
				for (var mdstt : mdstts) {
					// Get final stage results
					if (mdstt.isCompletedexecution() && mdstt.getTask().storage == MDCConstants.STORAGE.DISK) {
						Task task = mdstt.getTask();
						try (var fsstream = RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(task.jobid,
								getIntermediateDataFSFilePath(task.jobid, task.stageid, task.taskid), hdfs);
								var input = new Input(fsstream);) {
							var obj = kryofinal.readClassAndObject(input);
							writeOutputToFile(stageoutput.size(),obj);
							stageoutput.add(obj);
						} catch (Exception ex) {
							log.error(MassiveDataPipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
							throw ex;
						}
					}
				}
			}
			mdstts.clear();
			mdstts = null;
			return stageoutput;
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
			throw new PipelineException(MassiveDataPipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
		} finally {
			if(!Objects.isNull(kryofinal)) {
				 KryoPool.getKryoPool().free(kryofinal);
			}
		}

	}

	
	
	@SuppressWarnings("rawtypes")
	public void writeOutputToFile(int partcount, Object result)
			throws PipelineException {
		if (job.trigger == Job.TRIGGER.SAVERESULTSTOFILE) {
			try (var hdfs = FileSystem.get(new URI(job.uri), new Configuration());
					var fsdos = hdfs.create(
							new Path(job.uri.toString() + job.savepath + MDCConstants.HYPHEN + partcount));
					BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsdos))) {
				
				if(result instanceof List res) {
					for (var value : res) {
						bw.write(value.toString());
						bw.write(MDCConstants.NEWLINE);
					}
				}
				else {
					bw.write(result.toString());
				}
				bw.flush();
				fsdos.hflush();
			} catch (Exception ioe) {
				log.error(MassiveDataPipelineConstants.FILEIOERROR, ioe);
				throw new PipelineException(MassiveDataPipelineConstants.FILEIOERROR, ioe);
			}
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void writeOutputToFileInMemory(StreamPipelineTaskSubmitter mdstst,Kryo kryo,List stageoutput)
			throws PipelineException {
			try (var fsstream = getIntermediateInputStreamInMemory(mdstst.getTask());
					var input = new Input(fsstream);) {
				var obj = kryo.readClassAndObject(input);
				writeOutputToFile(stageoutput.size(),obj);
				stageoutput.add(obj);
			} catch (Exception ex) {
				log.error(MassiveDataPipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
				throw new PipelineException(MassiveDataPipelineConstants.FILEIOERROR, ex);
			}
	}
	/**
	 * Get the file streams from HDFS and jobid and stageid.
	 * 
	 * @param hdfs
	 * @param jobstage
	 * @return
	 * @throws Exception
	 */
	private InputStream getIntermediateInputStreamInMemory(Task task) throws Exception {
		try {
			var rdf = new RemoteDataFetch();
			rdf.jobid = task.jobid;
			rdf.stageid = task.stageid;
			rdf.taskid = task.taskid;
			rdf.hp = task.hostport;
			RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
			return new SnappyInputStream(new ByteArrayInputStream(rdf.data));
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
			throw new PipelineException(MassiveDataPipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
		}
	}
	public String getIntermediateDataFSFilePath(String jobid, String stageid, String taskid) {
		return (jobid + MDCConstants.HYPHEN + stageid + MDCConstants.HYPHEN + taskid + MDCConstants.DATAFILEEXTN);
	}

	/**
	 * Get the file streams from HDFS and jobid and stageid.
	 * 
	 * @param hdfs
	 * @param jobstage
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void writeOutputToHDFS(FileSystem hdfs, Task task, int partition, List stageoutput) throws Exception {
		try {
			var path = MDCConstants.BACKWARD_SLASH + FileSystemSupport.MDS + MDCConstants.BACKWARD_SLASH + task.jobid
					+ MDCConstants.BACKWARD_SLASH + (task.jobid + MDCConstants.HYPHEN + task.stageid
							+ MDCConstants.HYPHEN + task.taskid + MDCConstants.DATAFILEEXTN);
			log.debug("Forming URL Final Stage:" + MDCConstants.BACKWARD_SLASH + FileSystemSupport.MDS
					+ MDCConstants.BACKWARD_SLASH + task.jobid + MDCConstants.BACKWARD_SLASH
					+ (task.jobid + MDCConstants.HYPHEN + task.stageid + MDCConstants.HYPHEN + task.taskid
							+ MDCConstants.DATAFILEEXTN));			
			try (var input = new Input(new SnappyInputStream(new BufferedInputStream(hdfs.open(new Path(path)))));) {
				stageoutput.add(kryo.readClassAndObject(input));
			} catch (Exception ex) {
				log.error(MassiveDataPipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
				throw new PipelineException(MassiveDataPipelineConstants.FILEIOERROR, ex);
			}
		
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.JOBSCHEDULERHDFSDATAFETCHERROR, ex);
			throw new PipelineException(MassiveDataPipelineConstants.JOBSCHEDULERHDFSDATAFETCHERROR, ex);
		}
	}

	/**
	 * Get the file streams from HDFS and jobid and stageid.
	 * 
	 * @param hdfs
	 * @param jobstage
	 * @return
	 * @throws Exception
	 */
	private RemoteDataFetch getIntermediateRdfInMemory(Task task) throws Exception {
		try {
			var rdf = new RemoteDataFetch();
			rdf.jobid = task.jobid;
			rdf.stageid = task.stageid;
			rdf.taskid = task.taskid;
			rdf.hp = task.hostport;
			return rdf;
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
			throw new PipelineException(MassiveDataPipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
		}
	}

	/**
	 * Get intermediate stream obtained from ignite server.
	 * 
	 * @param jobstage
	 * @return InputStream object.
	 * @throws Exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void writeResultsFromIgnite(FileSystem hdfs, Task task,int partition, List stageoutput) throws Exception {
		try {
			log.info("Final Results Ignite Task: " + task);
			
			try (var sis = new SnappyInputStream(
					new ByteArrayInputStream(job.igcache.get(task.jobid + task.stageid + task.taskid)));
					var input = new Input(sis);) {
				var obj = kryo.readClassAndObject(input);				
				if (!Objects.isNull(job.uri)) {
					job.trigger = Job.TRIGGER.SAVERESULTSTOFILE;
					writeOutputToFile(partition,obj);
				}
				else {
					stageoutput.add(obj);
				}
			} catch (Exception ex) {
				log.error(MassiveDataPipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
				throw new PipelineException(MassiveDataPipelineConstants.FILEIOERROR, ex);
			}
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
			throw new PipelineException(MassiveDataPipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
		}
	}

	private String getIntermediateResultFS(Task task) throws Exception {
		return task.jobid + MDCConstants.HYPHEN + task.stageid + MDCConstants.HYPHEN + task.taskid
				+ MDCConstants.DATAFILEEXTN;
	}

	private int partitionindex = 0;

	/**
	 * Get the stream thread in order to execute the tasks.
	 * 
	 * @param jobid
	 * @param stageid
	 * @param input
	 * @param hbtss
	 * @param hpresmap
	 * @param stage
	 * @param partitionindex
	 * @param currentstage
	 * @param parentthreads
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	private StreamPipelineTaskSubmitter getMassiveDataStreamTaskSchedulerThread(String jobid, String stageid,
			Object input, Stage stage, int partitionindex, int currentstage, List<Object> parentthreads)
			throws PipelineException {
		try {
			stage.setStageid(stageid);
			var task = new Task();
			task.jobid = jobid;
			task.stageid = stage.id;
			task.storage = pipelineconfig.getStorage();
			String hp = null;
			if (currentstage == 0 || parentthreads == null) {
				if (input instanceof List inputl) {
					task.input = inputl.toArray();
					hp = ((BlocksLocation) task.input[0]).executorhp;
				} else if (input instanceof BlocksLocation bl) {
					hp = bl.executorhp;
					task.input = new Object[] { input };
				}
				task.parentremotedatafetch = null;

			} else {
				task.input = new Object[parentthreads.size()];
				task.parentremotedatafetch = new RemoteDataFetch[parentthreads.size()];
				for (var parentcount = 0; parentcount < parentthreads.size(); parentcount++) {
					if (parentthreads.get(parentcount) instanceof StreamPipelineTaskSubmitter mdstst) {
						if (!isignite) {
							task.parentremotedatafetch[parentcount] = new RemoteDataFetch();
							task.parentremotedatafetch[parentcount].jobid = mdstst.getTask().jobid;
							task.parentremotedatafetch[parentcount].stageid = mdstst.getTask().stageid;
							task.parentremotedatafetch[parentcount].taskid = mdstst.getTask().taskid;
							task.parentremotedatafetch[parentcount].hp = mdstst.getHostPort();
							hp = mdstst.getHostPort();
						} else {
							task.input[parentcount] = mdstst.getTask();
						}
					} else if (parentthreads.get(parentcount) instanceof BlocksLocation bl) {
						task.input[parentcount] = bl;
						hp = bl.executorhp;
					} else if (isignite && parentthreads.get(parentcount) instanceof Task insttask) {
						task.input[parentcount] = insttask;
					}
				}
			}
			var mdstst = new StreamPipelineTaskSubmitter(task, hp);
			task.hostport = hp;
			return mdstst;
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.JOBSCHEDULERCREATINGSTREAMSCHEDULERTHREAD, ex);
			throw new PipelineException(
					MassiveDataPipelineConstants.JOBSCHEDULERCREATINGSTREAMSCHEDULERTHREAD, ex);
		}
	}

	public void ping(Job job) throws Exception {
		chtssha = TssHAChannel.tsshachannel;
		var kryo = Utils.getKryoNonDeflateSerializer();
		kryo.register(StreamPipelineTaskSubmitter.class);
		jobping.execute(() -> {
			while (!istaskcancelled.get()) {
				try (var baos = new ByteArrayOutputStream();
						var lzf = new SnappyOutputStream(baos);
						var output = new Output(lzf);) {
					job.pipelineconfig = (PipelineConfig) job.pipelineconfig.clone();
					job.pipelineconfig.setOutput(null);
					Utils.writeKryoOutputClassObject(kryo, output, job);
					chtssha.send(new ObjectMessage(null, baos.toByteArray()));
					Thread.sleep(1000);
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
	 * @param hpresmap
	 * @return
	 * @throws Exception
	 */
	public String getTaskExecutorBalanced(long currentexecutor) throws Exception {
		try {
			var executorsl = new ArrayList<String>(taskexecutors);
			return executorsl.get((int) (currentexecutor % executorsl.size()));
		} catch (Exception ex) {
			log.error(MassiveDataPipelineConstants.JOBSCHEDULERGETTINGTASKEXECUTORLOADBALANCEDERROR, ex);
			throw new PipelineException(
					MassiveDataPipelineConstants.JOBSCHEDULERGETTINGTASKEXECUTORLOADBALANCEDERROR, ex);
		}
	}
}
