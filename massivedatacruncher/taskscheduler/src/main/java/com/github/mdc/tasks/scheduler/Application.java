package com.github.mdc.tasks.scheduler;

public interface Application {
	public abstract void runMRJob(String[] args, JobConfiguration jobconfiguration);
}
