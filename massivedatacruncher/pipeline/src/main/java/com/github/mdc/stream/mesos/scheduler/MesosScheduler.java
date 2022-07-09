package com.github.mdc.stream.mesos.scheduler;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.jgrapht.Graphs;
import org.jgrapht.graph.SimpleDirectedGraph;

import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.DAGEdge;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MesosThirdPartyLibraryDistributor;
import com.github.mdc.common.Task;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.scheduler.StreamPipelineTaskSubmitter;
import com.google.protobuf.ByteString;

/**
 * 
 * @author Arun
 * The mesos scheduler to schedule the tasks.
 */
public class MesosScheduler implements Scheduler {
	private static Logger log = Logger.getLogger(MesosScheduler.class);
	private List<StreamPipelineTaskSubmitter> mdststs;
	private SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph;
	private int taskIdCounter;

	private Protos.Credential credential;
	private ExecutorInfo executorinfo;
	private int finishedTasks;
	private MesosThirdPartyLibraryDistributor mtpld;
	private Map<String, StreamPipelineTaskSubmitter> jobstagemdsthread;
	int port;
	byte[] mrjar;

	private MesosScheduler(List<StreamPipelineTaskSubmitter> mdststs,
			SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph,
			Map<String, StreamPipelineTaskSubmitter> jobstagemdsthread,
			byte[] mrjar) {
		this.mdststs = mdststs;
		this.graph = graph;
		this.jobstagemdsthread = jobstagemdsthread;
		this.mrjar = mrjar;
		mtpld =  new MesosThirdPartyLibraryDistributor(MDCConstants.MESOS_CONFIGDIR);
		try {
			port = mtpld.start();
			log.debug("Properties server started:" + port);
		} catch (Throwable ex) {
			log.debug("Unable to start properties server:", ex);
		}
	}

	@Override
	public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {

	}

	@Override
	public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {

	}

	/**
	 * The resource offerings from mesos master to this scheduler.
	 * The offers will be CPU_PER_TASK and MEM_PER_TASK
	 */
	@Override
	public void resourceOffers(SchedulerDriver driver, List<Offer> offers)  {
		final var CPUS_PER_TASK = 1;
		final var MEM_PER_TASK = 2048;
		//Offers list.
		for (var offer : offers) {
			var offerCpus = 0;
			var offerMem = 0;
			//Calculate the total number of cpus and memory from the resources list of offer.  
			for (var resource : offer.getResourcesList()) {
				if (resource.getName().equals(MDCConstants.CPUS)) {
					offerCpus += resource.getScalar().getValue();
				} else if (resource.getName().equals(MDCConstants.MEM)) {
					offerMem += resource.getScalar().getValue();
				}
			}
			log.debug("Received Offer : " + offer.getId().getValue() + " with cpus = " + offerCpus + " and mem ="
					+ offerMem);

			var remainingCpus = offerCpus;
			var remainingMem = offerMem;
			var kryo = Utils.getKryoMesos();
			//Check whether the current offers is suitable to execute the tasks. 
			if (taskIdCounter < mdststs.size() && remainingCpus >= CPUS_PER_TASK && remainingMem >= MEM_PER_TASK) {
				try {
					var task = mdststs.get(taskIdCounter).getTask();
					//Get the task id of the job and stage.
					var taskId = buildNewTaskID(task);
					var baos = new ByteArrayOutputStream();
					var finalbaos = new ByteArrayOutputStream();
					var mdstst = jobstagemdsthread
							.get(task.jobid + task.stageid);
					var toexecute = true;
					//Get the predecessor list (i.e parent tasks) of the current node task in grap.
					var predessorslist = Graphs.predecessorListOf(graph, mdstst);
					for (var succcount = 0; succcount < predessorslist.size(); succcount++) {
						var predthread = predessorslist.get(succcount);
						if (!predthread.isCompletedexecution()) {
							toexecute = false;
							break;
						}

					}
					//Check if to execute the tasks.
					if (toexecute) {
						var finaloutput = new Output(finalbaos);
						kryo.writeClassAndObject(finaloutput, mrjar);
						finaloutput.flush();
						var interoutput = new Output(baos);
						kryo.writeClassAndObject(interoutput, task);
						interoutput.flush();
						interoutput.close();
						kryo.writeClassAndObject(finaloutput, baos.toByteArray());
						finaloutput.flush();
						finaloutput.close();
						//Get the task proto object.
						var taskprotos = Protos.TaskInfo.newBuilder().setName(MDCConstants.MESOS_TASK + taskId).setTaskId(taskId)
								.setSlaveId(offer.getSlaveId()).addResources(buildResource(MDCConstants.CPUS, CPUS_PER_TASK))
								.addResources(buildResource(MDCConstants.MEM, MEM_PER_TASK))
								.setData(ByteString.copyFrom(finalbaos.toByteArray()))
								.setExecutor(Protos.ExecutorInfo.newBuilder(executorinfo)).build();
						//Launch the task to be executed by the executor fo the offer.
						log.debug("Launching Stage Tasks: " + task.jobid + task.stageid);
						launchTask(driver, offer, taskprotos);
					}
				} catch (Exception ex) {
					log.error("Launching tasks failed: See cause below \n", ex);
				}

			}
		}

	}

	/**
	 * Obtain the new task  by job and stage id.
	 * @param jobstage
	 * @return
	 */
	private Protos.TaskID buildNewTaskID(Task task) {
		return Protos.TaskID.newBuilder()
				.setValue(task.jobid + task.stageid).build();
	}

	/**
	 * Lanuch the tasks to be executed by the mesos task executors.
	 * @param schedulerDriver
	 * @param offer
	 * @param task
	 */
	private void launchTask(SchedulerDriver schedulerDriver,
			Protos.Offer offer, Protos.TaskInfo task) {
		var tasks = new ArrayList<Protos.TaskInfo>();
		var offerIDs = new ArrayList<Protos.OfferID>();
		tasks.add(task);
		offerIDs.add(offer.getId());
		schedulerDriver.launchTasks(offerIDs, tasks);
		taskIdCounter++;
	}

	/**
	 * Scalar values to build for resource such as CPU or Memory proto object.
	 * @param name
	 * @param value
	 * @return
	 */
	private Protos.Resource buildResource(String name, double value) {
		return Protos.Resource.newBuilder().setName(name)
				.setType(Protos.Value.Type.SCALAR)
				.setScalar(buildScalar(value)).build();
	}

	private Protos.Value.Scalar.Builder buildScalar(double value) {
		return Protos.Value.Scalar.newBuilder().setValue(value);
	}

	@Override
	public void offerRescinded(SchedulerDriver driver, OfferID offerId) {

	}

	/**
	 * The method is invoked whether the tasks been completed by executor.
	 * Task Failed,Finished,Lost or Killed are tge task statuses.
	 */
	@Override
	public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
		log.debug("Stage Execution Status update: Recieved Status " + status.getState() + " from "
				+ status.getTaskId().getValue());
		//Task status check for completed.
		if (status.getState() == TaskState.TASK_FINISHED) {
			finishedTasks++;

			jobstagemdsthread.get(status.getTaskId().getValue()).setCompletedexecution(true);
			if (finishedTasks == mdststs.size()) {
				try {
					mtpld.stop();
				} catch (Throwable ex) {
					log.debug("Mesos Property Server stop error, See Cause below \n", ex);
				}
				driver.stop();
			}
		}
		//Check for the task is lost, killed or failed.
			if (status.getState() == TaskState.TASK_LOST
				|| status.getState() == TaskState.TASK_KILLED
				|| status.getState() == TaskState.TASK_FAILED) {
			log.debug("Aborting because task " + status.getTaskId().getValue()
					+ " is in unexpected state "
					+ status.getState().getValueDescriptor().getName()
					+ " with reason '"
					+ status.getReason().getValueDescriptor().getName() + MDCConstants.SINGLE_QUOTES
					+ " from source '"
					+ status.getSource().getValueDescriptor().getName() + MDCConstants.SINGLE_QUOTES
					+ " with message '" + status.getMessage() + MDCConstants.SINGLE_QUOTES);
			driver.abort();
		}

	}


	@Override
	public void frameworkMessage(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, byte[] data) {


	}

	@Override
	public void disconnected(SchedulerDriver driver) {

	}

	@Override
	public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {

	}

	@Override
	public void executorLost(SchedulerDriver driver, ExecutorID executorId, SlaveID slaveId, int status) {

	}

	@Override
	public void error(SchedulerDriver driver, String message) {

	}

	/**
	 * Framework builder.
	 * @return
	 */
	private FrameworkInfo getFrameworkInfo() {
		var builder = FrameworkInfo.newBuilder();
		builder.setUser("");
		builder.setName(MDCConstants.MESOS_FRAMEWORK_NAME);
		if (System.getenv(MDCConstants.MESOS_CHECKPOINT) != null) {
			log.debug("Enabling checkpoint for the MassiveDataCruncher framework");
			builder.setCheckpoint(true);
		}
		//Check for mesos authentication.
		if (System.getenv(MDCConstants.MESOS_AUTHENTICATE) != null) {
			log.debug("Enabling authentication for the MassiveDataCruncher framework");

			if (System.getenv(MDCConstants.DEFAULT_PRINCIPAL) == null) {
				log.debug("Expecting authentication principal, Scheduler to quit");
				System.exit(1);
			}

			if (System.getenv(MDCConstants.DEFAULT_SECRET) == null) {
				log.debug("Expecting authentication secret, Scheduler to quit");
				System.exit(1);
			}

			credential = Protos.Credential.newBuilder()
					.setPrincipal(System.getenv(MDCConstants.DEFAULT_PRINCIPAL)).setSecret(
					System.getenv(MDCConstants.DEFAULT_SECRET)).build();

			builder.setPrincipal(System.getenv(MDCConstants.DEFAULT_PRINCIPAL));


		} else {
			builder.setPrincipal(MDCConstants.MESOS_FRAMEWORK_NAME);
		}
		//Builder to build the object.
		return builder.build();
	}

	/**
	 * Mesos framework shaded jar URI path command builder. 
	 * @return
	 */
		private CommandInfo.URI getUri() {
		var uriBuilder = CommandInfo.URI.newBuilder();
		uriBuilder.setValue(MDCConstants.MESOS_FRAMEWORK_SHADED_JAR_PATH);
		uriBuilder.setExtract(false);
		return uriBuilder.build();
	}

	public String[] getProperties() {
		var file = new File(MDCConstants.MESOS_CONFIGDIR);
		return file.list((File dir, String name) ->
				name.endsWith(MDCConstants.PROPERTIESEXTN)
		);
	}

	public ExecutorInfo getExecutorinfo() {
		return executorinfo;
	}

	public void setExecutorinfo(ExecutorInfo executorinfo) {
		this.executorinfo = executorinfo;
	}

	/**
	 * Command builder
	 * @return
	 * @throws UnknownHostException 
	 * @throws Throwable
	 */
	private CommandInfo getCommandInfo() throws UnknownHostException {
		var cmdInfoBuilder = Protos.CommandInfo.newBuilder();
		cmdInfoBuilder.addUris(getUri());
		var stb = new StringBuilder();
		var props = getProperties();
		for (var prop :props) {
			stb.append(prop);
			stb.append(MDCConstants.COMMA);
		}
		var commasepprops = stb.toString();
		cmdInfoBuilder.setValue(MDCConstants.MESOS_FRAMEWORK_TASK_EXECUTOR_COMMAND + MDCConstants.SINGLESPACE + MDCConstants.HTTP + InetAddress.getLocalHost().getHostAddress() + MDCConstants.COLON + port + MDCConstants.SINGLESPACE + commasepprops.substring(0, commasepprops.length() - 1));
		log.debug(MDCConstants.MESOS_FRAMEWORK_TASK_EXECUTOR_COMMAND + MDCConstants.SINGLESPACE + MDCConstants.HTTP + InetAddress.getLocalHost().getHostAddress() + MDCConstants.COLON + port + MDCConstants.SINGLESPACE + commasepprops.substring(0, commasepprops.length() - 1));
		return cmdInfoBuilder.build();
	}

	/**
	 * Mesos executor information builder.
	 * @return
	 * @throws UnknownHostException 
	 * @throws Throwable
	 */
		public ExecutorInfo getExecutorInfos() throws UnknownHostException {
		var builder = ExecutorInfo.newBuilder();
		builder.setExecutorId(Protos.ExecutorID.newBuilder().setValue(MDCConstants.MESOS_FRAMEWORK_EXECUTOR_NAME));
		builder.setCommand(getCommandInfo());
		builder.setName(MDCConstants.MESOS_FRAMEWORK_EXECUTOR_NAME);
		return builder.build();
	}

	/**
		 Run the mesos framework by passing all the job and stages information.
		 @param mdststs
		 @param graph
		 @param mesosMaster
		 @param jobstagemdsthread
		 @throws UnknownHostException 
		 @throws Throwable
		*/
	public static void runFramework(List<StreamPipelineTaskSubmitter> mdststs,
			SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph, String mesosMaster,
			Map<String, StreamPipelineTaskSubmitter> jobstagemdsthread,
			byte[] mrjar) throws UnknownHostException {
		var scheduler = new MesosScheduler(mdststs, graph, jobstagemdsthread, mrjar);
		scheduler.setExecutorinfo(scheduler.getExecutorInfos());
		MesosSchedulerDriver driver;
		var frameworkinfo = scheduler.getFrameworkInfo();
		//Initialize the mesos framework via driver with credentials.
		if (scheduler.credential != null) {
			driver = new MesosSchedulerDriver(scheduler, frameworkinfo, mesosMaster, scheduler.credential);
		}
		//Initialize the mesos framework if otherwise.
		else {
			driver = new MesosSchedulerDriver(scheduler, frameworkinfo, mesosMaster, true);
		}
		//Run the mesos framework via driver.
		driver.run();
		//Stop the mesos driver.
		driver.stop();
	}
}
