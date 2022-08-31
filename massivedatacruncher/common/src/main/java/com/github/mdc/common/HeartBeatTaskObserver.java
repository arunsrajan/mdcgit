package com.github.mdc.common;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.Map;
import java.util.concurrent.*;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.log4j.Logger;

import static java.util.Objects.nonNull;

/**
 *
 * @author Arun
 * Heartbeat implementation with observer pattern for notification
 * to task scheduler to submit next tasks.
 */
public class HeartBeatTaskObserver<T> {

    static Logger log = Logger.getLogger(HeartBeatTaskObserver.class);
    boolean isstarted;
    Semaphore startmutex = new Semaphore(1);
    Map<T, PropertyChangeListener> taskpropmap = new ConcurrentHashMap<>();

    /**
     * This method adds multiple property change listener implementation to the observer.
     * @param listener
     */
    public synchronized void addPropertyChangeListener(T key, PropertyChangeListener listener) {
        log.debug("Entered HeartBeatTaskObserver.addPropertyChangeListener");
        taskpropmap.put(key, listener);
        log.debug("Exiting HeartBeatTaskObserver.addPropertyChangeListener");
    }

    /**
     * This function removes the property change listener from the observer.
     * @param key
     */
    public synchronized void removePropertyChangeListener(T key) {
        log.debug("Entered HeartBeatTaskObserver.removePropertyChangeListener");
        taskpropmap.remove(key);
        log.debug("Exiting HeartBeatTaskObserver.removePropertyChangeListener");
    }

    public synchronized void clearAll() {
        log.debug("Entered HeartBeatTaskObserver.removePropertyChangeListener");
        taskpropmap.clear();
        log.debug("Exiting HeartBeatTaskObserver.removePropertyChangeListener");
    }

    private ConcurrentLinkedQueue<T> objectqueue = new ConcurrentLinkedQueue<>();
    private ExecutorService thrpool = Executors.newFixedThreadPool(1);

    private CancellableRunnable throbject;

    /**
     * This method adds the Type parameter object to the queue. 
     * @param object
     * @throws InterruptedException
     */
    public synchronized void addToQueue(T object) throws InterruptedException {
        log.debug("Entered HeartBeatTaskObserver.addToQueue");
        objectqueue.offer(object);
        log.debug("Exiting HeartBeatTaskObserver.addToQueue");
    }

    /**
     * This method clears all the object from the queue.
     */
    public synchronized void clearQueue() {
        log.debug("Entered HeartBeatTaskObserver.clearQueue");
        objectqueue.clear();
        log.debug("Exiting HeartBeatTaskObserver.clearQueue");
    }

    /**
     * This method notifies change in the typed parameter object and triggers notification to the 
     * property change listeners.  
     * @param object
     */
    public synchronized void fireAndRemove(T object) {
        log.debug("Entered HeartBeatTaskObserver.triggerNotification");
        taskpropmap.get(object).propertyChange(new PropertyChangeEvent(this, "taskid", object, object));
        taskpropmap.remove(object);
        log.debug("Exiting HeartBeatTaskObserver.triggerNotification");
    }

    /**
     * This methods starts the Heartbeat observers.
     * @throws Exception
     */
    public void start() throws Exception {
        log.debug("Entered HeartBeatTaskObserver.start");
        startmutex.acquire();
        if (!isstarted) {
            throbject = new CancellableRunnable() {
                private MutableBoolean bool = new MutableBoolean();

                @Override
                public void run() {
                    bool.setValue(true);
                    while (bool.isTrue()) {
                        try {
                            T objecttotrigger = objectqueue.poll();
                            if(nonNull(objecttotrigger)) {
                                log.info("Object received: " + objecttotrigger);
                                fireAndRemove(objecttotrigger);
                            }
                            Thread.sleep(300);
                        }
                        catch (Exception e) {
                            log.error("Exiting HeartBeatTaskObserver.stop");
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
        log.debug("Exiting HeartBeatTaskObserver.start");
    }

    /**
     * Stop the heart beat observers.
     */
    public void stop() {
        log.debug("Entered HeartBeatTaskObserver.stop");
        if (throbject != null) {
            throbject.cancel();
        }
        thrpool.shutdown();
        log.debug("Exiting HeartBeatTaskObserver.stop");
    }
}
