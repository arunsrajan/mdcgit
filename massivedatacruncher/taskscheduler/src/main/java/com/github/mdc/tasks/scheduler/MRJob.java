package com.github.mdc.tasks.scheduler;

public interface MRJob {
	public abstract void runMRJob(String[] args, JobConfiguration jobconfiguration);
}
