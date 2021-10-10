package com.github.mdc.common;

import org.apache.curator.framework.CuratorFramework;

public interface TaskSchedulerCombinerSubmitterMBean {
	public abstract void setHostPort(String hp);
	public abstract String getHostPort();
	public abstract long getCombinerSubmittedCount();
	public abstract CuratorFramework getCuratorFramework();
}
