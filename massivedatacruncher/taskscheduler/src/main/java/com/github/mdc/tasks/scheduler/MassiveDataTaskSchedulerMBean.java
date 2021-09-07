package com.github.mdc.tasks.scheduler;

public interface MassiveDataTaskSchedulerMBean {
	public abstract Integer getNumberOfTasks();
	public abstract Integer getNumberIfMapperScheduled();
	public abstract Integer getNumberIfReducerScheduled();
}
