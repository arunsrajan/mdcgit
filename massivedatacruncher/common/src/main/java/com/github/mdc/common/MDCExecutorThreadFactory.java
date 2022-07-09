package com.github.mdc.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * @author Arun
 * Thread factory to create threads
 */
public class MDCExecutorThreadFactory implements ThreadFactory {

	private int priority;
	private boolean daemon;
	private final String namePrefix;
	private static final AtomicInteger poolNumber = new AtomicInteger(1);
	private final AtomicInteger threadNumber = new AtomicInteger(1);

	public MDCExecutorThreadFactory(int priority) {
		this(priority, true);
	}

	@SuppressWarnings("ucd")
	public MDCExecutorThreadFactory(int priority, boolean daemon) {
		this.priority = priority;
		this.daemon = daemon;
		namePrefix = MDCConstants.JOBPOOL + MDCConstants.HYPHEN + poolNumber.getAndIncrement() + MDCConstants.HYPHEN + MDCConstants.THREAD + MDCConstants.HYPHEN;
	}

	/**
		* Create a new thread
		*/
	@Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
		t.setDaemon(daemon);
		t.setPriority(priority);
		return t;
	}

}
