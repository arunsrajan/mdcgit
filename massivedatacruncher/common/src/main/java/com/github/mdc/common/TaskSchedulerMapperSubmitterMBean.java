package com.github.mdc.common;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;

public interface TaskSchedulerMapperSubmitterMBean {
	public abstract void setHostPort(String hp);
	public abstract String getHostPort();
	public abstract CuratorFramework getCuratorFramework();
	public abstract List<String> getContainers();
}
