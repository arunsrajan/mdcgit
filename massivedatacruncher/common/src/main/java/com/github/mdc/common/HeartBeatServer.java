package com.github.mdc.common;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.Receiver;
import org.jgroups.View;
/**
 * 
 * @author Arun
 * The Heartbeat for sending task statuses map reduce frameworks task executors and task schedulers
 */
public sealed class HeartBeatServer implements HeartBeatServerMBean,HeartBeatCloseable permits HeartBeatTaskScheduler {
	ExecutorService threadpool;
	ExecutorService scheduledthreadpool;
	JChannel channel;
	int rescheduledelay = 5000;
	int initialdelay = 5000;
	int pingdelay = 5000;
	String networkaddress;
	@SuppressWarnings("rawtypes")
	public ConcurrentMap hpresmap = new ConcurrentHashMap<>();
	ConcurrentMap<String, Timer> timermap = new ConcurrentHashMap<>();

	Timer pingtimer;
	
	
	

	static Logger log = Logger.getLogger(HeartBeatServer.class);
	protected int serverport;
	private String clusterid;
	public List<String> containers;
	/**
	 * This method initialized the heartbeat before the start.
	 */
	@Override
	public void init(Object... config) throws Exception {
		log.debug("Entered HeartBeatServer.init");
		if (config.length < 6) {
			throw new Exception(MDCConstants.HEARTBEAT_EXCEPTION_MESSAGE);
		}
		if(config[0] instanceof Integer rd) {
			rescheduledelay = rd;
		}
		else {
			throw new Exception(MDCConstants.HEARTBEAT_EXCEPTION_RESCHEDULE_DELAY);
		}
		if(config[1] instanceof Integer sp) {
			serverport = sp;
		}
		else {
			throw new Exception(MDCConstants.HEARTBEAT_EXCEPTION_SERVER_PORT);
		}
		if(config[2] instanceof String na) {
			networkaddress = na;
		}else {
			throw new Exception(MDCConstants.HEARTBEAT_EXCEPTION_SERVER_HOST);
		}
		if(config[3] instanceof Integer id) {
			initialdelay = id;
		}else {
			throw new Exception(MDCConstants.HEARTBEAT_EXCEPTION_INITIAL_DELAY);
		}
		if(config[4] instanceof Integer pd) {
			pingdelay = pd;
		}else {
			throw new Exception(MDCConstants.HEARTBEAT_EXCEPTION_PING_DELAY);
		}
		
		if(config[5] instanceof String cid) {
			clusterid = cid;
		}
		else {
			throw new HeartBeatException(MDCConstants.HEARTBEAT_EXCEPTION_CONTAINER_ID);
		}
		
		
		
		
	
		threadpool = Executors.newWorkStealingPool();
		log.debug("Exiting HeartBeatServer.init");
	}
	View oldView = null;
	/**
	 * Start the heartbeat to get notification from executors.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void start() throws Exception {
		log.debug("Entered HeartBeatServer.start");
		scheduledthreadpool = Executors.newWorkStealingPool();
		channel = Utils.getChannelWithPStack(networkaddress);
		channel.setName(networkaddress + MDCConstants.UNDERSCORE + serverport);		
		channel.setReceiver(new Receiver() {
			public void viewAccepted(View newView) {
				log.debug("Entered Receiver.viewAccepted");
				var addresses = newView.getMembers();
				var schedulerHostPort = networkaddress + MDCConstants.UNDERSCORE + serverport;
				var nodes = addresses.stream().map(address->address.toString()).
						filter(addresss->!addresss.equals(schedulerHostPort)).collect(Collectors.toList());
				if(clusterid==null ||clusterid.trim().equals(MDCConstants.EMPTY)) {
					MDCNodes.put(nodes);
					if(MDCNodesResources.get()!=null) {
						var leftmembers = View.leftMembers(oldView, newView);
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
						if (timermap.get(resources.getNodeport()) == null) {
							var timer = new Timer();
							timermap.put(resources.nodeport, timer);
							timer.schedule(new LocalTimerTask(resources.nodeport, timer, hpresmap, timermap,
									scheduledthreadpool, HeartBeatServer.this), initialdelay,
									rescheduledelay);
						}
						hpresmap.put(resources.getNodeport(), resources);
						log.debug("Exiting Receiver.receive");
					} catch (Exception e) {
						log.error("Unable to receive and process resources, See below for the cause: ",e);
					}
				}
			});
		channel.setDiscardOwnMessages(true);
		if(clusterid!=null && !clusterid.trim().equals(MDCConstants.EMPTY)) {
			channel.connect(clusterid);
		}
		else {
			MDCNodesResources.put(hpresmap);
			channel.connect(MDCConstants.TS +MDCConstants.HYPHEN+ MDCProperties.get().getProperty(MDCConstants.CLUSTERNAME));
		}
		log.debug("Exiting HeartBeatServer.start");
	}

	/**
	 * Stop heartbeat to receive the notification.
	 */
	@Override
	public void stop() throws Exception {
		try {
			log.debug("Entered HeartBeatServer.stop");
			if (threadpool != null) {
				threadpool.shutdown();
				try {
				    if (!threadpool.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				    	threadpool.shutdownNow();
				    	threadpool.shutdown();
				    	Thread.sleep(2000);
				    } 
				} catch (Exception e) {
					log.error("Thread Pool shutdown error, See Cause below: \n",e);
					threadpool.shutdownNow();
				}
			}
			if (scheduledthreadpool != null) {
				scheduledthreadpool.shutdown();
				try {
				    if (!scheduledthreadpool.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
				    	scheduledthreadpool.shutdownNow();
				    	scheduledthreadpool.shutdown();
				    	Thread.sleep(2000);
				    } 
				} catch (Exception e) {
					log.error("Scheduled Thread Pool shutdown error, See Cause below: \n",e);
					scheduledthreadpool.shutdownNow();
				}
			}
			var keys = timermap.keySet();
			for (var key : keys) {
				var timertopurge = timermap.remove(key);
				if (timertopurge != null) {
					timertopurge.cancel();
					timertopurge.purge();
				}
			}
			if(pingtimer!=null) {
				pingtimer.cancel();
				pingtimer.purge();
			}
			log.debug("Exiting HeartBeatServer.stop");
		} catch (Exception ex) {
			log.error("Heartbeat stop or shutdown error, See Cause below: \n",ex);
		}
	}

	/**
	 * Destroys the heartbeat jgroups channel.
	 */
	@Override
	public void destroy() throws Exception {
		try {
			log.debug("Entered HeartBeatServer.destroy");
			if(!Objects.isNull(channel))channel.close();
			log.debug("Exiting HeartBeatServer.destroy");
		} catch (Exception ex) {
			log.error("Heartbeat destroy error, See Cause below: \n",ex);
		}
	}

	/**
	 * Broadcast the message to the peers via jgroups channel.
	 */
	@Override
	public void send(byte[] msg) throws Exception {
		log.debug("Entered HeartBeatServer.send");
		channel.send(new ObjectMessage(null,msg));
		log.debug("Exiting HeartBeatServer.send");
	}

	/**
	 * ping the resource information from task executors to task scheduler.
	 */
	@Override
	public void ping() throws Exception {
		log.debug("Entered HeartBeatServer.ping");
		channel = Utils.getChannelWithPStack(networkaddress);
		channel.setName(networkaddress + MDCConstants.UNDERSCORE + serverport);
		if(clusterid!=null && !clusterid.trim().equals(MDCConstants.EMPTY)) {
			channel.connect(clusterid);
		}
		else {
			channel.connect(MDCConstants.TS +MDCConstants.HYPHEN+MDCProperties.get().getProperty(MDCConstants.CLUSTERNAME));
		}
		var runtime = Runtime.getRuntime();
		var resources = new Resources();
		pingtimer = new Timer();
		pingtimer.schedule(new TimerTask(){

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
					channel.send(new ObjectMessage(null,resources));
				} catch (Exception ex) {
					log.error("Heartbeat ping error, See Cause below: \n",ex);
				}
			}			
		}, pingdelay, pingdelay);
		log.debug("Exiting HeartBeatServer.ping");
	}

	/**
	 * This function returns the current total diskspace.
	 * @return disk space
	 */
	@SuppressWarnings("static-access")
	private Double totaldiskspace() {
		log.debug("Entered HeartBeatServer.totaldiskspace");
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
		log.debug("Exiting HeartBeatServer.totaldiskspace");
		return totalHDSize;
	}

	/**
	 * This function returns the current usable diskspace.
	 * @return disk space
	 */
	@SuppressWarnings("static-access")
	private Double usablediskspace() {
		log.debug("Entered HeartBeatServer.usablediskspace");
		var file = new File(MDCConstants.SLASH);
		var values = new ArrayList<Double>();
		var list = file.listRoots();
		for (var driver : list) {
			var driveGB = driver.getUsableSpace() / (double)MDCConstants.GB;
			values.add(driveGB);
		}
		var totalHDSize = 0d;
		for (var i = 0; i < values.size(); i++) {
			totalHDSize += values.get(i);
		}
		log.debug("Exiting HeartBeatServer.usablediskspace");
		return totalHDSize;
	}
	
	/**
	 * This function returns the current available physical memory.
	 * @return physical memory
	 */
	public Long getTotalAvailablePhysicalMemory() {
		log.debug("Entered HeartBeatServer.getTotalAvailablePhysicalMemory");
		var os = (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory
				.getOperatingSystemMXBean();
		var availablePhysicalMemorySize = os.getFreePhysicalMemorySize();
		log.debug("Exiting HeartBeatServer.getTotalAvailablePhysicalMemory");
		return availablePhysicalMemorySize;
	}
	
	/**
	 * This function returns the total physical memory.
	 * @return physical memory
	 */
	public Long getPhysicalMemory() {
		log.debug("Entered HeartBeatServer.getPhysicalMemory");
		var os = (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory
				.getOperatingSystemMXBean();
		var physicalMemorySize = os.getTotalPhysicalMemorySize();
		log.debug("Exiting HeartBeatServer.getPhysicalMemory");
		return physicalMemorySize;
	}
	/**
	 * 
	 * @author Arun
	 * Timer task scheduler to obtain resource information from task executors
	 */
	public static class LocalTimerTask extends TimerTask {

		private String key;
		Map hpresmap, timermap;
		private Resources prevresources = null;
		@SuppressWarnings("rawtypes") LocalTimerTask(String key, Timer timer, Map hpresmap, Map timermap,
				 ExecutorService scheduledthreadpool,
				HeartBeatServer hbs) {
			this.key = key;
			this.hpresmap = hpresmap;
			this.timermap = timermap;
		}

		/**
		 * This method is called by the timer task repeatedly for 
		 * removing the resources if the timer exceeds the scheduled delay
		 * and didnot receive the resource information from task executors.
		 */
		@Override
		public void run() {
			log.debug("Entered LocalTimerTask.send");
			var object = hpresmap.get(key);
			if (object != null && object instanceof Resources resources) {
				if ((prevresources != null && prevresources == resources) || prevresources == null) {
					hpresmap.remove(key);
				}
				prevresources = resources;
			}
			log.debug("Exiting LocalTimerTask.send");
		}
		
	}
	@Override
	public void close() throws IOException {
	}

}