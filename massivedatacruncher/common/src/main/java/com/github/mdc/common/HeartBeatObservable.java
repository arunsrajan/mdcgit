package com.github.mdc.common;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.log4j.Logger;

/**
 * 
 * @author Arun 
 * Heartbeat implementation with observer pattern for notification
 * to task scheduler to submit next tasks.
 */
public class HeartBeatObservable<T> {

	static Logger log = Logger.getLogger(HeartBeatObservable.class);
	boolean isstarted = false;
	Semaphore startmutex = new Semaphore(1);
	PropertyChangeSupport propchangesupport = new PropertyChangeSupport(this);

	/**
	 * This method adds multiple property change listener implementation to the observer.
	 * @param listener
	 */
	public synchronized void addPropertyChangeListener(PropertyChangeListener listener) {
		log.debug("Entered HeartBeatObservable.addPropertyChangeListener");
		propchangesupport.addPropertyChangeListener(listener);
		log.debug("Exiting HeartBeatObservable.addPropertyChangeListener");
	}

	/**
	 * This function removes the property change listener from the observer.
	 * @param listener
	 */
	public synchronized void removePropertyChangeListener(PropertyChangeListener listener) {
		log.debug("Entered HeartBeatObservable.removePropertyChangeListener");
		propchangesupport.removePropertyChangeListener(listener);
		log.debug("Exiting HeartBeatObservable.removePropertyChangeListener");
	}

	private LinkedBlockingQueue<T> objectqueue = new LinkedBlockingQueue<>();
	private ExecutorService thrpool = Executors.newWorkStealingPool();

	private CancellableRunnable throbject;

	/**
	 * This method adds the Type parameter object to the queue. 
	 * @param object
	 * @throws InterruptedException
	 */
	public synchronized void addToQueue(T object) throws InterruptedException {
		log.debug("Entered HeartBeatObservable.addToQueue");
		objectqueue.put(object);
		log.debug("Exiting HeartBeatObservable.addToQueue");
	}

	/**
	 * This method clears all the object from the queue.
	 */
	public synchronized void clearQueue() {
		log.debug("Entered HeartBeatObservable.clearQueue");
		objectqueue.clear();
		log.debug("Exiting HeartBeatObservable.clearQueue");
	}
	
	/**
	 * This method removes all the typed parameter objects from the queue.
	 */
	public synchronized void removePropertyChangeListeners() {
		log.debug("Entered HeartBeatObservable.removePropertyChangeListeners");
		var propchangelisten = propchangesupport.getPropertyChangeListeners();
		for(var proplisten:propchangelisten) {
			propchangesupport.removePropertyChangeListener(proplisten);
		}
		log.debug("Exiting HeartBeatObservable.removePropertyChangeListeners");
	}
	
	/**
	 * This method notifies change in the typed parameter object and triggers notification to the 
	 * property change listeners.  
	 * @param object
	 */
	public synchronized void triggerNotification(T object) {
		log.debug("Entered HeartBeatObservable.triggerNotification");
		propchangesupport.firePropertyChange("stageid", null, object);
		log.debug("Exiting HeartBeatObservable.triggerNotification");
	}

	/**
	 * This methods starts the Heartbeat observers.
	 * @throws Exception
	 */
	public void start() throws Exception {
		log.debug("Entered HeartBeatObservable.start");
		startmutex.acquire();
		if (!isstarted) {
			throbject = new CancellableRunnable() {
				private MutableBoolean bool = new MutableBoolean();

				@Override
				public void run() {
					bool.setValue(true);
					while (bool.isTrue()) {
						try {
							T objecttotrigger = objectqueue.take();
							log.info("Object Queued will be triggered: " + objecttotrigger);
							triggerNotification(objecttotrigger);
						}
						catch (InterruptedException e) {
							log.warn("Interrupted!", e);
						    // Restore interrupted state...
						    Thread.currentThread().interrupt();
						}
						catch (Exception e) {
							log.error("Exiting HeartBeatObservable.stop");
						}
					}
				}

				@Override
				public void cancel() {
					bool.setValue(false);
				}
			};
			// Start the Heartbeat notification queue.
			thrpool.execute(throbject);
			isstarted = true;
		}
		startmutex.release();
		log.debug("Exiting HeartBeatObservable.start");
	}

	/**
	 * Stop the heart beat observers.
	 */
	public void stop() {
		log.debug("Entered HeartBeatObservable.stop");
		if (throbject != null) {
			throbject.cancel();
		}
		thrpool.shutdown();
		log.debug("Exiting HeartBeatObservable.stop");
	}
}
