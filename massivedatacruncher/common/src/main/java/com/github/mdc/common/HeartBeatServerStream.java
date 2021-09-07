package com.github.mdc.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.Receiver;
import org.jgroups.View;


/**
 * 
 * @author Arun 
 * The Heartbeat server for messaging between task scheduler and
 * task executors implemented using jgroups.
 */
public sealed class HeartBeatServerStream implements HeartBeatServerMBean,HeartBeatCloseable permits HeartBeatTaskSchedulerStream {
	ExecutorService threadpool;
	ExecutorService scheduledthreadpool;
	JChannel channel;
	int serverport;
	int rescheduledelay = 5000;
	int initialdelay = 5000;
	int pingdelay = 5000;
	String networkaddress;
	@SuppressWarnings("rawtypes") ConcurrentMap hpresmap = new ConcurrentHashMap<>();
	ConcurrentMap<String, Timer> timermap = new ConcurrentHashMap<>();

	Timer pingtimer;
	Semaphore semaphore = new Semaphore(1);
	private static Logger log = Logger.getLogger(HeartBeatServerStream.class);

	ConcurrentMap<String, Callable<Object>> jobstagemap = new ConcurrentHashMap<>();
	private String clusterid;
	public List<String> containers;
	
	/**
	 * This method initializes heartbeat.
	 */
	@Override
	public void init(Object... config) throws Exception {
		log.debug("Entered HeartBeatServerStream.init");
		if (config.length < 6) {
			throw new HeartBeatException(MDCConstants.HEARTBEAT_EXCEPTION_MESSAGE);
		}
		if (config[0] instanceof Integer rd) {
			rescheduledelay = rd;
		} 
		else {
			throw new HeartBeatException(MDCConstants.HEARTBEAT_EXCEPTION_RESCHEDULE_DELAY);
		}
		if (config[1] instanceof Integer sp) {
			serverport = sp;
		} else {
			throw new HeartBeatException(MDCConstants.HEARTBEAT_EXCEPTION_SERVER_PORT);
		}
		if (config[2] instanceof String na) {
			networkaddress = na;
		} else {
			throw new HeartBeatException(MDCConstants.HEARTBEAT_EXCEPTION_SERVER_HOST);
		}
		if(config[3] instanceof Integer id) {
			initialdelay = id;
		}else {
			throw new HeartBeatException(MDCConstants.HEARTBEAT_EXCEPTION_INITIAL_DELAY);
		}
		if(config[4] instanceof Integer pd) {
			pingdelay = pd;
		}else {
			throw new HeartBeatException(MDCConstants.HEARTBEAT_EXCEPTION_PING_DELAY);
		}
		if(config[5] instanceof String cid) {
			clusterid = cid;
		}else {
			throw new HeartBeatException(MDCConstants.HEARTBEAT_EXCEPTION_CONTAINER_ID);
		}
		threadpool = Executors.newWorkStealingPool();
		log.debug("Exiting HeartBeatServerStream.init");
	}
	View oldView = null;
	
	/**
	 * Start the server to receive updates from task executor streaming server.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void start() throws Exception {
		log.debug("Entered HeartBeatServerStream.start");
		scheduledthreadpool = Executors.newWorkStealingPool();
		channel = Utils.getChannelWithPStack(networkaddress);
		channel.setName(networkaddress + MDCConstants.UNDERSCORE + serverport);
		channel.setReceiver(new Receiver() {
			public void viewAccepted(View newView) {
				log.debug("Entered Receiver.viewAccepted");
				log.debug("Nodes View: "+newView.getMembers());
				var addresses = newView.getMembers();
				
				var schedulerHostPort = networkaddress + MDCConstants.UNDERSCORE + serverport;
				var nodes = addresses.stream().map(address->address.toString()).
						filter(addresss->!addresss.equals(schedulerHostPort)).collect(Collectors.toList());
				if(clusterid==null ||clusterid.trim().equals(MDCConstants.EMPTY)) {
					MDCNodes.put(nodes);
					if(MDCNodesResources.get()!=null) {
						List<Address> leftmembers = View.leftMembers(oldView, newView);
						if(leftmembers!=null) {
							nodes = leftmembers.stream().map(address->address.toString()).
									filter(addresss->!addresss.equals(schedulerHostPort)).collect(Collectors.toList());
							MDCNodesResources.get().keySet().retainAll(nodes);
						}
					}
				}
				else {
					containers = nodes;
				}
				oldView = newView;
				log.debug("Exiting Receiver.viewAccepted");
			}
			public void receive(Message msg) {
				try {
					log.debug("Entered Receiver.receive");
				Resources resources = msg.getObject();
				// Create a timer in scheduler if timer for tasks to receive updates from
				// executor has not been created.
				log.debug("Resources: "+resources+" "+resources.getNodeport()+" "+timermap);
				if (resources.getNodeport()!=null && timermap.get(resources.getNodeport()) == null) {
					var timer = new Timer();
					timermap.put(resources.nodeport, timer);
					timer.schedule(
							new LocalTimerTask(resources.nodeport, timer, hpresmap, timermap, jobstagemap,
									scheduledthreadpool, HeartBeatServerStream.this, semaphore, 0, 5000),
							initialdelay, rescheduledelay);
				}
				if(resources.getNodeport()!=null) {
					hpresmap.put(resources.getNodeport(), resources);
				}
				log.debug("Exiting Receiver.receive");
			} catch (Exception e) {
				log.error("Unable to receive and process resources, See below for the cause: ", e);
			}
		    }
		});
		channel.setDiscardOwnMessages(true);
		if(clusterid!=null && !clusterid.trim().equals(MDCConstants.EMPTY)) {
			channel.connect(clusterid);
		}
		else {
			MDCNodesResources.put(hpresmap);
			channel.connect(MDCConstants.TSS +MDCConstants.HYPHEN+MDCProperties.get().getProperty(MDCConstants.CLUSTERNAME));
		}
		log.debug("Exiting HeartBeatServerStream.start");
	}

	/**
	 * Stop the timer if the tasks has been completed
	 */
	@Override
	public void stop() throws Exception {
		try {
			log.debug("Entered HeartBeatServerStream.stop");
			if (threadpool != null) {
				threadpool.shutdown();
				shutdownThreadPool(threadpool);
			}
			if (scheduledthreadpool != null) {
				scheduledthreadpool.shutdown();
				shutdownThreadPool(scheduledthreadpool);
			}
			var keys = timermap.keySet();
			for (var key : keys) {
				var timertopurge = timermap.remove(key);
				if (timertopurge != null) {
					timertopurge.cancel();
					timertopurge.purge();
				}
			}
			if (pingtimer != null) {
				pingtimer.cancel();
				pingtimer.purge();
			}
			log.debug("Exiting HeartBeatServerStream.stop");
		} catch (Exception ex) {
			log.error("Heartbeat stop or shutdown error, See Cause below: \n", ex);
		}
	}

	/**
	 * This method shutsdown the executor created by the heartbeat functions.
	 * @param threadpool
	 */
	private void shutdownThreadPool(ExecutorService threadpool) {
		try {
			log.debug("Entered HeartBeatServerStream.shutdownThreadPool");
			if (!threadpool.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				threadpool.shutdownNow();
				threadpool.shutdown();
				Thread.sleep(2000);
			}
			log.debug("Exiting HeartBeatServerStream.shutdownThreadPool");
		} catch (Exception e) {
			log.error("Thread Pool shutdown error, See Cause below: \n", e);
			threadpool.shutdownNow();
		}
	}
	
	/**
	 * Destroys the heartbeat jgroups channel.
	 */
	@Override
	public void destroy() throws Exception {
		try {
			log.debug("Entered HeartBeatServerStream.destroy");
			if(channel!=null) {
				channel.close();
			}
			log.debug("Exiting HeartBeatServerStream.destroy");
		} catch (Exception ex) {
			log.error("Heartbeat destroy error, See Cause below: \n", ex);
		}
	}

	/**
	 * Broadcast the message to the peers via jgroups channel.
	 */
	@Override
	public void send(byte[] msg) throws Exception {
		log.debug("Entered HeartBeatServerStream.send");
		channel.send(new ObjectMessage(null,msg));
		log.debug("Exiting HeartBeatServerStream.send");
	}

	

	/**
	 * Ping the resource information availability in task executor to task scheduler
	 * heartbeat server
	 */
	@Override
	public void ping() throws Exception {
		log.debug("Entered HeartBeatServerStream.ping");
		channel = Utils.getChannelWithPStack(networkaddress);
		channel.setName(networkaddress + MDCConstants.UNDERSCORE + serverport);
		if(clusterid!=null && !clusterid.trim().equals(MDCConstants.EMPTY)) {
			channel.connect(clusterid);
		}
		else {
			channel.connect(MDCConstants.TSS +MDCConstants.HYPHEN+MDCProperties.get().getProperty(MDCConstants.CLUSTERNAME));
		}
		var runtime = Runtime.getRuntime();
		var resources = new Resources();
		pingtimer = new Timer();

		// Timer tasks scheduler to send updates frequently.
		pingtimer.schedule(new TimerTask() {

			@Override
			public void run() {
				resources.nodeport = networkaddress + MDCConstants.UNDERSCORE + serverport;
				resources.totalmemory = runtime.totalMemory();
				resources.freememory = getTotalAvailablePhysicalMemory();
				resources.numberofprocessors = runtime.availableProcessors();
				resources.totaldisksize = totaldiskspace();
				resources.usabledisksize = usablediskspace();
				resources.physicalmemorysize = getPhysicalMemory();
				try {
					channel.send(new ObjectMessage(null, resources));
				} catch (Exception ex) {
					log.debug("Heartbeat ping error, See Cause below: \n", ex);
				}
			}
		}, pingdelay, pingdelay);
		log.debug("Exiting HeartBeatServerStream.ping");
	}
	
	/**
	 * Total disk space in task executors machine.
	 * 
	 * @return
	 */
	private Double totaldiskspace() {
		log.debug("Entered HeartBeatServerStream.totaldiskspace");
		var file = new File(MDCConstants.SLASH);
		var values = new ArrayList<Double>();
		var list = file.listRoots();
		for (var driver : list) {
			var driveGB = driver.getTotalSpace() / (double)MDCConstants.GB;
			values.add(driveGB);
		}
		var totalHDSize = 0d;
		for (var i = 0; i < values.size(); i++) {
			totalHDSize += values.get(i);
		}
		log.debug("Exiting HeartBeatServerStream.totaldiskspace");
		return totalHDSize;
	}

	/**
	 * Usable disk space in Task executors machine.
	 * 
	 * @return
	 */
	private Double usablediskspace() {
		log.debug("Entered HeartBeatServerStream.usablediskspace");
		var file = new File(MDCConstants.SLASH);
		var values = new ArrayList<Double>();
		var list = file.listRoots();
		for (var driver : list) {
			var driveGB = driver.getUsableSpace() / (double)MDCConstants.GB;
			values.add(driveGB);
		}
		Double totalHDSize = 0d;
		for (var i = 0; i < values.size(); i++) {
			totalHDSize += values.get(i);
		}
		log.debug("Exiting HeartBeatServerStream.usablediskspace");
		return totalHDSize;
	}

	/**
	 * Physical memory availability of task executor machine.
	 * 
	 * @return physical memory
	 */
	public Long getPhysicalMemory() {
		log.debug("Entered HeartBeatServerStream.getPhysicalMemory");
		var os = (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory
				.getOperatingSystemMXBean();
		var physicalMemorySize = os.getTotalPhysicalMemorySize();
		log.debug("Exiting HeartBeatServerStream.getPhysicalMemory");
		return physicalMemorySize;
	}
	
	/**
	 * Total Available physical memory.
	 * @return physical memory
	 */
	public Long getTotalAvailablePhysicalMemory() {
		log.debug("Entered HeartBeatServerStream.getTotalAvailablePhysicalMemory");
		var os = (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory
				.getOperatingSystemMXBean();
		var availablePhysicalMemorySize = os.getFreePhysicalMemorySize();
		log.debug("Exiting HeartBeatServerStream.getTotalAvailablePhysicalMemory");
		return availablePhysicalMemorySize;
	}
	/**
	 * Timer task for receiving the updates on resources and job execution stages
	 * status update.
	 * 
	 * @author Arun
	 *
	 */
	public static class LocalTimerTask extends TimerTask {

		private String key;
		private @SuppressWarnings("rawtypes")
		Map hpresmap;
		private Resources prevresources = null;
		@SuppressWarnings({ "rawtypes" }) 
		LocalTimerTask(String key, Timer timer, Map hpresmap, Map timermap,
				ConcurrentMap<String, Callable<Object>> jobstagemap, ExecutorService scheduledthreadpool,
				HeartBeatServerStream hbs, Semaphore semaphore, int rescheduledelayretry, int rescheduledelay) {
			this.key = key;
			this.hpresmap = hpresmap;
		}

		/**
		 * This method is called by the timer task repeatedly for 
		 * removing the resources if the timer exceeds the scheduled delay
		 * and didnot receive the resource information from task executors.
		 */
		@Override
		public void run() {
			try {
				log.debug("Entered LocalTimerTask.run");
				var object = hpresmap.get(key);
				// updates on resources.
				if (object instanceof Resources resources) {
					if ((prevresources != null && prevresources == resources) || prevresources == null) {
						hpresmap.remove(key);
					}
					prevresources = resources;
				}
				log.debug("Exiting LocalTimerTask.run");
			} catch (Exception ex) {
				log.error("Heartbeat Timertask error, See Cause below: \n", ex);
			}
		}
	}

	@Override
	public void close() throws IOException {
		try {
			log.debug("Entered HeartBeatServerStream.close");
			this.stop();
			this.destroy();
			log.debug("Exiting HeartBeatServerStream.close");
		} catch (Exception ex) {
			log.error("HeartBeat Server Stream stop destroy error", ex);
		}

	}
}
