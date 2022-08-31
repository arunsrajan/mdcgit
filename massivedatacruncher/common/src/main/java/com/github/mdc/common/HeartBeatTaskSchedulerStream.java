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
package com.github.mdc.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.Receiver;
import org.jgroups.View;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.Task.TaskStatus;

/**
 * 
 * @author Arun
 * The heart beat server for MR streaming api which 
 * receives the task status updates from streaming task executors.
 */
public final class HeartBeatTaskSchedulerStream extends HeartBeatServerStream implements HeartBeatCloseable {
	private static Logger log = Logger.getLogger(HeartBeatTaskSchedulerStream.class);
	private String jobid;
	private TaskStatus taskstatus = TaskStatus.SUBMITTED;
	private HeartBeatTaskObserver<Task> hbo;
	protected double timetakenseconds;
	public List<String> containers;

	public TaskStatus getTaskstatus() {
		return taskstatus;
	}

	public void setTaskstatus(TaskStatus taskstatus) {
		this.taskstatus = taskstatus;
	}

	public HeartBeatTaskObserver<Task> getHbo() {
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
		hbo = new HeartBeatTaskObserver<>();
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
								hbo.addToQueue(task);
								apptasks.add(task.taskid);
							} else if ((task.taskstatus == Task.TaskStatus.COMPLETED
									|| task.taskstatus == Task.TaskStatus.FAILED)) {
								var trs = new TaskResponseStatus();
								trs.jobid = task.jobid;
								trs.stageid = task.stageid;
								trs.taskid = task.taskid;
								try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
									Utils.writeKryoOutputClassObject(kryo, output, trs);
									if (!channel.isClosed()) {
										channel.send(new ObjectMessage(msg.getSrc(), baos.toByteArray()));
									}
								}
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

	Semaphore pingmutex = new Semaphore(1);
	boolean responsereceived;

	/**
	 * This method pings the status of the tasks executed by the task executors.
	 * @param stageid
	 * @param taskid
	 * @param hostport
	 * @param taskstatus
	 * @param timetaken
	 * @throws Exception
	 */
	public void pingOnce(String stageid, String taskid, String hostport, TaskStatus taskstatus, double timetaken, String stagefailuremessage)
			throws Exception {
		log.debug("Entered HeartBeatTaskSchedulerStream.pingOnce");
		pingmutex.acquire();
		try (var channel = Utils.getChannelWithPStack(
				NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)))) {
			log.info("Entered Pinging Message: " + jobid + MDCConstants.SINGLESPACE + stageid + MDCConstants.SINGLESPACE
					+ taskid + MDCConstants.SINGLESPACE + taskstatus);
			channel.setName(jobid + stageid);
			channel.setDiscardOwnMessages(true);
			channel.connect(jobid);
			var task = new Task();
			task.jobid = jobid;
			task.stageid = stageid;
			task.taskstatus = taskstatus;
			task.hostport = hostport;
			task.taskid = taskid;
			task.timetakenseconds = timetaken;
			task.stagefailuremessage = taskstatus == Task.TaskStatus.FAILED ? stagefailuremessage : "";
			// Initiate the tasks execution by sending the job stage
			// information.
			try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
				Utils.writeKryoOutputClassObject(Utils.getKryoSerializerDeserializer(), output, task);
				if (taskstatus == TaskStatus.COMPLETED) {
					responsereceived = false;
					channel.setReceiver(new Receiver() {
						public void viewAccepted(View clusterview) {
						}

						public void receive(Message msg) {
							var rawbuffer = (byte[]) ((ObjectMessage) msg).getObject();
							var kryo = Utils.getKryoSerializerDeserializer();
							try (var bais = new ByteArrayInputStream(rawbuffer);
									var input = new Input(bais);) {
								var obj = Utils.readKryoInputObjectWithClass(kryo, input);
								if (obj instanceof TaskResponseStatus trs) {
									if (trs.jobid.equals(jobid) && trs.stageid.equals(stageid)
											&& trs.taskid.equals(taskid)) {
										responsereceived = true;
									}
								}
							} catch (Exception ex) {
								log.error(MDCConstants.EMPTY, ex);
							}
						}
					});
					while (!responsereceived) {
						channel.send(new ObjectMessage(null, baos.toByteArray()));
						Thread.sleep(1000);
					}
				} else {
					channel.send(new ObjectMessage(null, baos.toByteArray()));
				}
			}

			log.info("Exiting Pinging Message: " + jobid + MDCConstants.SINGLESPACE + stageid + MDCConstants.SINGLESPACE
					+ taskid + MDCConstants.SINGLESPACE + taskstatus);
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.info("Heartbeat ping once error, See Cause below: \n", ex);
		}
		pingmutex.release();
		log.debug("Exiting HeartBeatTaskSchedulerStream.pingOnce");
	}

}
