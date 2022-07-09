package com.github.mdc.common;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;

public interface TaskSchedulerReducerSubmitterMBean {
	public abstract void setHostPort(String hp);

	public abstract String getHostPort();

	public abstract long getReducerSubmittedCount();

	public abstract CuratorFramework getCuratorFramework();

	public abstract List<String> getContainers();
}
