package com.github.mdc.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.Receiver;
import org.jgroups.View;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.ApplicationTask.TaskStatus;
import com.github.mdc.common.ApplicationTask.TaskType;
/**
 * 
 * @author Arun
 * Heart beat task scheduler to receive updates on Job execution from task executors to task schedulers.
 */
public final class HeartBeatTaskScheduler extends HeartBeatServer implements HeartBeatCloseable{
	
	private String applicationid,taskid;
	public TaskStatus taskstatus = TaskStatus.SUBMITTED;
	public TaskType tasktype = TaskType.MAPPERCOMBINER;
	private HeartBeatObservable<ApplicationTask> hbo;
	@SuppressWarnings("rawtypes")
	public ConcurrentMap<String, Callable<Context>> apptaskmdtstmmap = new ConcurrentHashMap<>();
	@SuppressWarnings("rawtypes")
	ConcurrentMap<String, Callable<Context>> apptaskmdtstcmap = new ConcurrentHashMap<>();
	@SuppressWarnings("rawtypes")
	public ConcurrentMap<String, Callable<Context>> apptaskmdtstrmap = new ConcurrentHashMap<>();
	
	/**
	 * This method initializes heartbeat.
	 */
	@Override
	public void init(Object... config) throws Exception {
		log.debug("Entered HeartBeatTaskScheduler.init");
		super.init(config);
		if(config.length!=8) {
			throw new Exception(MDCConstants.HEARTBEAT_TASK_SCHEDULER_EXCEPTION_MESSAGE);
		}
		if(config[6] instanceof String appid) {
			applicationid = appid;
			
		}else {
			throw new Exception(MDCConstants.HEARTBEAT_TASK_SCHEDULER_EXCEPTION_APPID);
		}
		
		if(config[7] instanceof String tid) {
			taskid = tid;
		}
		else {
			throw new Exception(MDCConstants.HEARTBEAT_TASK_SCHEDULER_EXCEPTION_TASKID);
		}
		
		log.debug("Exiting HeartBeatTaskScheduler.init");
	}
	
	
	
	/**
	 * Start the heart beat server to receive the updates on job execution statuses. 
	 */
	@Override
	public void start() throws Exception {
		log.debug("Entered HeartBeatTaskScheduler.start");
		hbo = new HeartBeatObservable<>();
		channel = Utils.getChannelWithPStack(networkaddress);
		channel.setName(applicationid + taskid);
		channel.setReceiver(new Receiver() {
			public void viewAccepted(View clusterview) {
				log.debug("View: " + clusterview);
			}

			public void receive(Message msg) {
				try {
					log.debug("Entered Receiver.receive");
					var kryo = Utils.getKryoNonDeflateSerializer();
					var rawbuffer = (byte[])((ObjectMessage)msg).getObject();
					try (var bais = new ByteArrayInputStream(rawbuffer);
							var input = new Input(bais);) {
						var apptask = (ApplicationTask) Utils.readKryoInputObjectWithClass(kryo, input);
						if (applicationid.equals(apptask.applicationid)) {
							if (apptask.taskstatus == ApplicationTask.TaskStatus.COMPLETED
									||apptask.taskstatus == ApplicationTask.TaskStatus.FAILED) {
								log.info("AppTask Before adding to queue: " + apptask);
								hbo.addToQueue(apptask);
								var mrjr = new MRJobResponse();
								mrjr.setAppid(apptask.applicationid);
								mrjr.setTaskid(apptask.taskid);
								try (var baos = new ByteArrayOutputStream();
										var output = new Output(baos);) {
									Utils.writeKryoOutputClassObject(kryo, output, mrjr);
									channel.send(new ObjectMessage(msg.getSrc(), baos.toByteArray()));
								} finally {

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
					log.error("Heartbeat Receive Updates error, See Cause below: \n", ex);
				}
			}
		});
		channel.setDiscardOwnMessages(true);
		channel.connect(applicationid);
		log.debug("Exiting HeartBeatTaskScheduler.start");
	}
	Semaphore pingmutex = new Semaphore(1);
	boolean responsereceived = false;
	
	/**
	 * This method pings the status of the tasks executed by the task executors.
	 * @param taskid
	 * @param taskstatus
	 * @param tasktype
	 * @throws Exception
	 */
	public synchronized void pingOnce(String taskid, TaskStatus taskstatus, TaskType tasktype, String apperrormessage) throws Exception {
		log.debug("Entered HeartBeatTaskScheduler.pingOnce");
		try (var channel = Utils.getChannelWithPStack(
				NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)))) {
			channel.setName(applicationid + taskid);
			channel.connect(applicationid);
			var apptask = new ApplicationTask();
			apptask.applicationid = applicationid;
			apptask.taskid = taskid;
			apptask.taskstatus = taskstatus;
			apptask.tasktype = tasktype;
			apptask.hp = this.networkaddress + MDCConstants.UNDERSCORE + this.serverport;
			apptask.apperrormessage = taskstatus==ApplicationTask.TaskStatus.FAILED?apperrormessage:"";
			try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
				Utils.writeKryoOutputClassObject(Utils.getKryoNonDeflateSerializer(), output, apptask);
				if (taskstatus == TaskStatus.COMPLETED) {
					responsereceived = false;
					channel.setReceiver(new Receiver() {
						public void viewAccepted(View clusterview) {
						}

						public void receive(Message msg) {
							var rawbuffer = (byte[])((ObjectMessage)msg).getObject();
							var kryo = Utils.getKryoNonDeflateSerializer();
							try (var bais = new ByteArrayInputStream(rawbuffer);
									var input = new Input(bais);) {
								var obj = Utils.readKryoInputObjectWithClass(kryo, input);
								if (obj instanceof MRJobResponse mrjr) {
									if (mrjr.getAppid().equals(applicationid) && mrjr.getTaskid().equals(taskid)) {
										responsereceived = true;
									}
								}
							} catch (Exception ex) {
								log.error(MDCConstants.EMPTY, ex);
							}
						}
					});
					while (true) {
						channel.send(new ObjectMessage(null, baos.toByteArray()));
						if(responsereceived) {
							break;
						}
					}
				} else {
					channel.send(new ObjectMessage(null, baos.toByteArray()));
				}
			} finally {

			}
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
		    // Restore interrupted state...
		    Thread.currentThread().interrupt();
		  } catch (Exception ex) {
			log.info("Heartbeat ping once error, See Cause below: \n", ex);
		}
		log.debug("Exiting HeartBeatTaskScheduler.pingOnce");
	}

	/**
	 * This method pings the status of the tasks executed by the task executors.
	 * @param apptask
	 * @param taskstatus
	 * @param tasktype
	 * @throws Exception
	 */
	public synchronized void pingOnce(ApplicationTask apptask, TaskStatus taskstatus, TaskType tasktype, String apperrormessage) throws Exception {
		log.debug("Entered HeartBeatTaskScheduler.pingOnce");
		try (var channel = Utils.getChannelWithPStack(
				NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)))) {
			channel.setName(apptask.applicationid+apptask.taskid);
			channel.connect(apptask.applicationid);
			apptask.taskstatus = taskstatus;
			apptask.tasktype = tasktype;
			apptask.apperrormessage = taskstatus==ApplicationTask.TaskStatus.FAILED?apperrormessage:"";
			try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
				Utils.writeKryoOutputClassObject(Utils.getKryoNonDeflateSerializer(), output, apptask);
				if (taskstatus == TaskStatus.COMPLETED) {
					responsereceived = false;
					channel.setReceiver(new Receiver() {
						public void viewAccepted(View clusterview) {
						}

						public void receive(Message msg) {
							var rawbuffer = (byte[])((ObjectMessage)msg).getObject();
							var kryo = Utils.getKryoNonDeflateSerializer();
							try (var bais = new ByteArrayInputStream(rawbuffer);
									var input = new Input(bais);) {
								var obj = Utils.readKryoInputObjectWithClass(kryo, input);
								if (obj instanceof MRJobResponse mrjr) {
									if (mrjr.getAppid().equals(applicationid) && mrjr.getTaskid().equals(taskid)) {
										responsereceived = true;
									}
								}
							} catch (Exception ex) {
								log.error(MDCConstants.EMPTY, ex);
							}
						}
					});
					while (true) {
						channel.send(new ObjectMessage(null, baos.toByteArray()));
						if(responsereceived) {
							break;
						}
					}
				} else {
					channel.send(new ObjectMessage(null, baos.toByteArray()));
				}
			} finally {

			}
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
		    // Restore interrupted state...
		    Thread.currentThread().interrupt();
		  } catch (Exception ex) {
			log.info("Heartbeat ping once error, See Cause below: \n", ex);
		}
		log.debug("Exiting HeartBeatTaskScheduler.pingOnce");
	}

	/**
	 * Get the heartbeat observable object created by the heartbeat.
	 * @return
	 */
	public HeartBeatObservable<ApplicationTask> getHbo() {
		return hbo;
	}
	
	public void stop() throws Exception {
		super.stop();
	}
	public void destroy() throws Exception {
		super.destroy();
	}
}
