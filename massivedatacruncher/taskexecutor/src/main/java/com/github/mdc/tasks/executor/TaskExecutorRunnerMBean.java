package com.github.mdc.tasks.executor;

public interface TaskExecutorRunnerMBean {
	public void init() throws Exception;
	public void start() throws Exception;
	public void destroy() throws Exception;
}
