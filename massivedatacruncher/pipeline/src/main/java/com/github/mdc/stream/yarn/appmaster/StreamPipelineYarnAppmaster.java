package com.github.mdc.stream.yarn.appmaster;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.jgrapht.Graphs;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.StaticEventingAppmaster;
import org.springframework.yarn.am.allocate.DefaultContainerAllocator;
import org.springframework.yarn.am.container.AbstractLauncher;

import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.ByteBufferPool;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.DAGEdge;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.Task;
import com.github.mdc.stream.scheduler.StreamPipelineTaskSubmitter;

/**
 * 
 * @author Arun Yarn App master with lifecycle init, submitapplication,
 *         isjobcomplete and prelaunch containers. Various container events
 *         captured are container failure and completed operation with container
 *         statuses.
 */
public class StreamPipelineYarnAppmaster extends StaticEventingAppmaster implements ContainerLauncherInterceptor {

	private static final Log log = LogFactory.getLog(StreamPipelineYarnAppmaster.class);
	private List<Task> tasks;

	private Map<String, Task> pendingtasks = new ConcurrentHashMap<>();
	private Map<String, Task> pendingsubmittedtasks = new ConcurrentHashMap<>();
	private Map<String, Timer> requestresponsetimer = new ConcurrentHashMap<>();
	private Map<String, Map<String, Task>> containertaskmap = new ConcurrentHashMap<>();
	private Map<String, JobStage> jsidjsmap;
	private Map<String, Boolean> sentjobstages = new ConcurrentHashMap<>();
	private final Object lock = new Object();

	private long taskidcounter;
	private long taskcompleted;
	private List<StreamPipelineTaskSubmitter> mdststs;
	private SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
			DAGEdge.class);
	private Map<String, StreamPipelineTaskSubmitter> taskmdsthread;

	/**
	 * Container initialization.
	 */
	@Override
	protected void onInit() throws Exception {
		super.onInit();
		if (getLauncher() instanceof AbstractLauncher launcher) {
			launcher.addInterceptor(this);
		}
	}

	/**
	 * Submit the user application. The various parameters obtained from HDFS are
	 * graph with node and edges, job stage map, job stage with task information.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void submitApplication() {
		try {
			var prop = new Properties();
			MDCProperties.put(prop);
			ByteBufferPoolDirect.init();
			ByteBufferPool.init(3);
			log.debug("Task Id Counter: " + taskidcounter);
			log.debug("Environment: " + getEnvironment());
			var yarninputfolder = MDCConstants.YARNINPUTFOLDER + MDCConstants.BACKWARD_SLASH
					+ getEnvironment().get(MDCConstants.YARNMDCJOBID);
			log.debug("Yarn Input Folder: " + yarninputfolder);
			log.debug("AppMaster HDFS: " + getConfiguration().get(MDCConstants.HDFSNAMENODEURL));
			var namenodeurl = getConfiguration().get(MDCConstants.HDFSNAMENODEURL);
			var containerallocator = (DefaultContainerAllocator) getAllocator();
			log.debug("Parameters: " + getParameters());
			log.debug("Container-Memory: " + getParameters().getProperty("container-memory", "1024"));
			containerallocator.setMemory(Integer.parseInt(getParameters().getProperty("container-memory", "1024")));
			System.setProperty(MDCConstants.HDFSNAMENODEURL, getConfiguration().get(MDCConstants.HDFSNAMENODEURL));
			// Thread containing the job stage information.
			mdststs = (List<StreamPipelineTaskSubmitter>) RemoteDataFetcher
					.readYarnAppmasterServiceDataFromDFS(namenodeurl, yarninputfolder, MDCConstants.MASSIVEDATA_YARNINPUT_DATAFILE);
			// Graph containing the nodes and edges.
			graph = (SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge>) RemoteDataFetcher
					.readYarnAppmasterServiceDataFromDFS(namenodeurl, yarninputfolder,
							MDCConstants.MASSIVEDATA_YARNINPUT_GRAPH_FILE);
			// task map.
			taskmdsthread = (Map<String, StreamPipelineTaskSubmitter>) RemoteDataFetcher
					.readYarnAppmasterServiceDataFromDFS(namenodeurl, yarninputfolder, MDCConstants.MASSIVEDATA_YARNINPUT_TASK_FILE);
			jsidjsmap = (Map<String, JobStage>) RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS(namenodeurl, yarninputfolder,
					MDCConstants.MASSIVEDATA_YARNINPUT_JOBSTAGE_FILE);
			tasks = mdststs.stream().map(StreamPipelineTaskSubmitter::getTask).collect(Collectors.toList());
			log.debug("tasks size:" + tasks.size());
		} catch (Exception ex) {
			log.debug("Submit Application Error, See cause below \n", ex);
		}
		var appmasterservice = (StreamPipelineYarnAppmasterService) getAppmasterService();
		log.debug("In SubmitApplication Setting AppMaster Service: " + appmasterservice);
		if (appmasterservice != null) {
			// Set the Yarn App master bean to the Yarn App master service object.
			appmasterservice.setYarnAppMaster(this);
		}
		super.submitApplication();
	}
	Map<String, String> containeridipmap = new ConcurrentHashMap<>();

	/**
	 * Set App Master service hosts and port running before the container is
	 * launched.
	 */
	@Override
	public ContainerLaunchContext preLaunch(Container container, ContainerLaunchContext context) {
		var service = getAppmasterService();
		if (service != null) {
			Map<String, String> env = null;
			try {
				containeridipmap.put(container.getId().toString().trim(), container.getNodeId().getHost());
				var port = service.getPort();
				var address = InetAddress.getLocalHost().getHostAddress();
				log.debug("App Master Service Ip Address: " + address);
				log.debug("App Master Service Port: " + port);
				env = new HashMap<>(context.getEnvironment());
				// Set the service port to the environment object.
				env.put(YarnSystemConstants.AMSERVICE_PORT, Integer.toString(port));

				// Set the service host to the environment object.
				env.put(YarnSystemConstants.AMSERVICE_HOST, address);
			} catch (Exception ex) {
				log.debug("Container Prelaunch error, See cause below \n", ex);
			}
			context.setEnvironment(env);
			return context;
		} else {
			return context;
		}
	}

	/**
	 * Execute the OnContainer completed method when container is exited with the
	 * exitcode.
	 */
	@Override
	protected void onContainerCompleted(ContainerStatus status) {
		synchronized (lock) {
			super.onContainerCompleted(status);
			log.debug("Container completed: " + status.getContainerId());
			if (containertaskmap.get(status.getContainerId().toString()) != null) {
				var jobidstageidjs = containertaskmap.get(status.getContainerId().toString());
				pendingtasks.keySet().removeAll(jobidstageidjs.keySet());
			}
			if (hasJobs()) {
				log.debug("Container completed: " + "Has jobs reallocating container for: " + status.getContainerId());
				getAllocator().allocateContainers(1);
			}
		}
	}

	/**
	 * Execute the OnContainer failed method when container is exited with the
	 * exitcode.
	 */
	@Override
	protected boolean onContainerFailed(ContainerStatus status) {
		synchronized (lock) {
			log.debug("Container failed: " + status.getContainerId());
			if (containertaskmap.get(status.getContainerId().toString()) != null) {
				pendingtasks.putAll(containertaskmap.get(status.getContainerId().toString()));
			}
			if (hasJobs()) {
				log.debug("Container failed: " + "Has jobs reallocating container for: " + status.getContainerId());
				getAllocator().allocateContainers(1);
			}
			return true;
		}
	}

	/**
	 * Check whether the job has been completed by the containers and app master to
	 * exit.
	 */
	@Override
	protected boolean isComplete() {
		return completedJobs();
	}

	/**
	 * Update the job statuses if job status is completed.
	 * 
	 * @param job
	 * @param success
	 */
	@SuppressWarnings("ucd")
	public void reportJobStatus(Task task, boolean success, String containerid) {
		synchronized (lock) {
			if (success) {
				taskcompleted++;
				log.debug(task.jobid + task.stageid + task.taskid + " Updated");
				taskmdsthread.get(task.jobid + task.stageid + task.taskid).setCompletedexecution(true);
				pendingtasks.remove(task.jobid + task.stageid + task.taskid);
				containertaskmap.get(containerid).remove(task.jobid + task.stageid + task.taskid);
			} else {
				pendingtasks.put(task.jobid + task.stageid + task.taskid, task);
			}
			pendingsubmittedtasks.remove(task.jobid + task.stageid + task.taskid);
		}
	}

	/**
	 * Obtain the job to execute
	 * 
	 * @return
	 */
	public Object getTask(String containerid) {
		synchronized (lock) {
			if (!sentjobstages.containsKey(containerid)) {
				sentjobstages.put(containerid, true);
				return jsidjsmap;
			}
			// If the pending jobs being staged first execute the pending jobs.
			if (pendingtasks.size() > 0) {
				var pendings = pendingtasks.keySet();
				for (var pend : pendings) {
					if (!pendingsubmittedtasks.containsKey(pend)) {
						pendingsubmittedtasks.put(pend, pendingtasks.get(pend));
						var timer = new Timer();
						var task = pendingtasks.get(pend);
						requestresponsetimer.put(task.jobid + task.stageid + task.taskid, timer);
						timer.schedule(new JobStageTimerTask(task, pendingtasks, pendingsubmittedtasks, lock,
								requestresponsetimer), 10000);
						return task;
					}
				}

			}
			// If the jobs stage submitted is less than the total jobs size
			// return the job to be executed.
			if (taskidcounter < tasks.size()) {
				var task = tasks.get((int) taskidcounter);
				String ip = containeridipmap.get(containerid.trim());
				if (!Objects.isNull(task.input)) {
					if (task.input[0] instanceof BlocksLocation) {
						var bl = (BlocksLocation) task.input[0];
						if (!Objects.isNull(bl.block) && bl.block.length > 0) {
							String[] blockip = bl.block[0].hp.split(MDCConstants.COLON);
							if (!ip.equals(blockip[0])) {
								return null;
							}
						}
					}
				}
				var toexecute = true;
				var predessorslist = Graphs.predecessorListOf(graph, taskmdsthread.get(task.jobid + task.stageid + task.taskid));
				for (var succcount = 0; succcount < predessorslist.size(); succcount++) {
					var predthread = predessorslist.get(succcount);
					if (!taskmdsthread.get(predthread.getTask().jobid + predthread.getTask().stageid + predthread.getTask().taskid)
							.isCompletedexecution()) {
						toexecute = false;
						break;
					}

				}
				if (toexecute) {
					if (!containertaskmap.containsKey(containerid)) {
						containertaskmap.put(containerid, new ConcurrentHashMap<>());
					}
					containertaskmap.get(containerid).put(task.jobid + task.stageid + task.taskid, task);
					log.debug(
							"Allocating JobsStage " + task.jobid + task.stageid + task.taskid + " to the container: " + containerid);
					taskidcounter++;
					var timer = new Timer();
					requestresponsetimer.put(task.jobid + task.stageid + task.taskid, timer);
					timer.schedule(new JobStageTimerTask(task, pendingtasks, pendingsubmittedtasks, lock,
							requestresponsetimer), 10000);
					log.debug(task + " To Execute, Task Counter:" + taskidcounter);
					return task;
				} else {
					log.debug(task + " To StandBy, Task Counter:" + taskidcounter);
					return null;
				}
			}
			return null;
		}
	}

	public void requestRecieved(Task task) {
		synchronized (lock) {
			var timer = requestresponsetimer.get(task.jobid + task.stageid + task.taskid);
			timer.cancel();
			timer.purge();
			requestresponsetimer.remove(task.jobid + task.stageid + task.taskid);
		}
	}

	/**
	 * Check on whether the jobs are available to execute.
	 * 
	 * @return
	 */
	public boolean hasJobs() {
		synchronized (lock) {
			log.debug("Has Jobs: " + (taskidcounter < tasks.size()) + ", Task Counter:" + taskidcounter);
			return taskidcounter < tasks.size() || pendingtasks.size() > 0 || taskcompleted < tasks.size();
		}
	}

	/**
	 * Check whether the jobs has been completed.
	 * 
	 * @return
	 */
	private boolean completedJobs() {
		synchronized (lock) {
			log.debug("Completed Jobs: " + (taskcompleted >= tasks.size()) + ", Task Completeed Counter:"
					+ taskcompleted);
			return taskcompleted >= tasks.size();
		}
	}

	@Override
	public String toString() {
		return MDCConstants.PENDINGJOBS + MDCConstants.EQUAL + pendingtasks.size() + MDCConstants.SINGLESPACE
				+ MDCConstants.RUNNINGJOBS + MDCConstants.EQUAL + pendingtasks.size();
	}

	static class JobStageTimerTask extends TimerTask {
		private Task task;
		private Map<String, Task> pendingtasks;
		Object locktimer;
		private Map<String, Timer> requestresponsetimer;
		private Map<String, Task> pendingsubmittedtasks;

		private JobStageTimerTask(Task task, Map<String, Task> pendingtasks, Map<String, Task> pendingsubmittedtasks,
				Object locktimer, Map<String, Timer> requestresponsetimer) {
			this.task = task;
			this.pendingtasks = pendingtasks;
			this.locktimer = locktimer;
			this.requestresponsetimer = requestresponsetimer;
			this.pendingsubmittedtasks = pendingsubmittedtasks;
		}

		@Override
		public void run() {
			synchronized (locktimer) {
				var timer = requestresponsetimer.get(task.jobid + task.stageid + task.taskid);
				timer.cancel();
				timer.purge();
				log.debug("");
				pendingsubmittedtasks.remove(task.jobid + task.stageid + task.taskid);
				pendingtasks.put(task.jobid + task.stageid + task.taskid, task);
				log.debug("Response Not Received:" + task.jobid + task.stageid + task.taskid + " Putting In Pending Jobs");
			}
		}

	}
}
