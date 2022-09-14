/*
 * CopyronNight 2021 the original author or authors.
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
package com.github.mdc.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;
import org.jgroups.*;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.Task.TaskStatus;
import org.jgroups.stack.IpAddress;

import static java.util.Objects.nonNull;

/**
 * 
 * @author Arun
 * The heart beat server for MR streaming api which 
 * receives the task status updates from streaming task executors.
 */
public final class HeartBeatTaskSchedulerStream extends HeartBeatStream implements HeartBeatCloseable {
	private static Logger log = Logger.getLogger(HeartBeatTaskSchedulerStream.class);
	private String jobid;
	private TaskStatus taskstatus = TaskStatus.SUBMITTED;
	private Map<String, HeartBeatTaskObserver<Task>> hbo;
	protected double timetakenseconds;
	public List<String> containers;

	public TaskStatus getTaskstatus() {
		return taskstatus;
	}

	public void setTaskstatus(TaskStatus taskstatus) {
		this.taskstatus = taskstatus;
	}

	public Map<String, HeartBeatTaskObserver<Task>> getHbo() {
		return hbo;
	}

	public double getTimetakenseconds() {
		return timetakenseconds;
	}

	public void setTimetakenseconds(double timetakenseconds) {
		this.timetakenseconds = timetakenseconds;
	}

	/**
	 * This method initializes heartbeat.
	 */
	@Override
	public void init(Object... config) throws Exception {
		log.debug("Entered HeartBeatTaskSchedulerStream.init");
		super.init(config);
		if (config.length != 7) {
			throw new HeartBeatException(MDCConstants.HEARTBEAT_TASK_SCHEDULER_STREAM_EXCEPTON_MESSAGE);
		}
		if (config[6] instanceof String jid) {
			jobid = jid;
		}
		else {
			throw new HeartBeatException(MDCConstants.HEARTBEAT_TASK_SCHEDULER_STREAM_EXCEPTON_JOBID);
		}
		hbo = new ConcurrentHashMap<>();
		log.debug("Exiting HeartBeatTaskSchedulerStream.init");
	}


	/**
	 * Start the heart beat server in task schedulers 
	 * to receive the task updates.
	 */
	@Override
	public void start() throws Exception {
		log.debug("Entered HeartBeatTaskSchedulerStream.start");
		channel = Utils.getChannelWithPStack(networkaddress);
		channel.setName(jobid);
		channel.setDiscardOwnMessages(true);
		channel.setReceiver(new Receiver() {

			@Override
			public void viewAccepted(View clusterview) {
				log.debug("Nodes View: " + clusterview.getMembers());
			}
			List<String> apptasks = new ArrayList<>();
			public void receive(Message msg) {
				try {
					log.debug("Entered Receiver.receive");
					var rawbuffer = (byte[]) ((ObjectMessage) msg).getObject();
					var kryo = Utils.getKryoSerializerDeserializer();
					try (var bais = new ByteArrayInputStream(rawbuffer); var input = new Input(bais);) {
						var task = (Task) Utils.readKryoInputObjectWithClass(kryo, input);
						log.info("Task Status: " + task + MDCConstants.SINGLESPACE + task.taskstatus);
						if (jobid.equals(task.jobid)) {
							if ((task.taskstatus == Task.TaskStatus.COMPLETED
									|| task.taskstatus == Task.TaskStatus.FAILED)
									&& !apptasks.contains(task.taskid)) {
								log.info("Task adding to queue: " + task);
								hbo.get(task.getHostport()).addToQueue(task);
								apptasks.add(task.taskid);
							}
						}
						log.debug("Exiting Receiver.receive");
					}
				} catch (InterruptedException e) {
					log.warn("Interrupted!", e);
					// Restore interrupted state...
					Thread.currentThread().interrupt();
				} catch (Exception ex) {
					log.info("Heartbeat Receive Updates error, See Cause below: \n", ex);
				}
			}
		});
		channel.setDiscardOwnMessages(true);
		channel.connect(jobid);
		log.debug("Exiting HeartBeatTaskSchedulerStream.start");
	}

	public void clearStageJobStageMap() {
		hpresmap.clear();
	}

	/**
	 * Gets Physical address of heartbeat channel.
	 * @return
	 */
	public String getPhysicalAddress() {
		if (nonNull(channel)) {
			PhysicalAddress physicalAddress = (PhysicalAddress)
					channel.down(
							new Event(
									Event.GET_PHYSICAL_ADDRESS, channel.getAddress()
							)
					);
			return physicalAddress.printIpAddress();
		}
		throw new IllegalStateException("HeartBeat channel needs to be initialized");
	}

	Semaphore pingmutex = new Semaphore(1);
	boolean responsereceived;

	/**
	 * This method pings the status of the tasks executed by the task executors.
	 * @param tasksource
	 * @param taskstatus
	 * @param startendtime
	 * @param timetaken
	 * @param stagefailuremessage
	 * @throws Exception
	 */
	public void pingOnce(Task tasksource, TaskStatus taskstatus, Long[] startendtime, double timetaken, String stagefailuremessage)
			throws Exception {
		log.debug("Entered HeartBeatTaskSchedulerStream.pingOnce");
		try (var channel = Utils.getChannelWithPStack(
				NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)))) {
			log.info("Entered Pinging Message: " + jobid + MDCConstants.SINGLESPACE + tasksource.stageid + MDCConstants.SINGLESPACE
					+ tasksource.taskid + MDCConstants.SINGLESPACE + taskstatus);
			channel.setName(jobid + tasksource.stageid);
			channel.setDiscardOwnMessages(true);
			channel.connect(jobid);
			var task = new Task();
			task.jobid = jobid;
			task.stageid = tasksource.stageid;
			task.taskstatus = taskstatus;
			task.hostport = tasksource.hostport;
			task.taskid = tasksource.taskid;
			task.timetakenseconds = timetaken;
			task.stagefailuremessage = taskstatus == Task.TaskStatus.FAILED ? stagefailuremessage : "";
			task.taskexecutionstartime = startendtime[0];
			task.taskexecutionendtime = startendtime[1];
			// Initiate the tasks execution by sending the job stage
			// information.
			try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
					Utils.writeKryoOutputClassObject(Utils.getKryoSerializerDeserializer(), output, task);
					IpAddress ip = new IpAddress(tasksource.hbphysicaladdress);
					channel.send(new ObjectMessage(ip, baos.toByteArray()));
			}

			log.info("Exiting Pinging Message: " + jobid + MDCConstants.SINGLESPACE + tasksource.stageid + MDCConstants.SINGLESPACE
					+ tasksource.taskid + MDCConstants.SINGLESPACE + taskstatus);
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.info("Heartbeat ping once error, See Cause below: \n", ex);
		}
		log.debug("Exiting HeartBeatTaskSchedulerStream.pingOnce");
	}

	/**
	 * The resources destroy method
	 */
	public void destroy() throws Exception {
		super.destroy();
		if(nonNull(hbo)) {
			hbo.values().parallelStream().forEach(hbto->{
				hbto.stop();
			});
		}
	}

}
