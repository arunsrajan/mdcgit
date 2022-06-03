package com.github.mdc.tasks.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;

import com.github.mdc.common.Context;
import com.github.mdc.common.TaskSchedulerReducerSubmitterMBean;
import com.github.mdc.common.ReducerValues;
import com.github.mdc.common.Utils;

@SuppressWarnings("rawtypes")
public class TaskSchedulerReducerSubmitter
		implements TaskSchedulerReducerSubmitterMBean, Callable<Context> {
	private static Logger log = Logger.getLogger(TaskSchedulerReducerSubmitter.class);
	ReducerValues rv;
	String hp;
	String applicationid;
	String taskid;
	long reducersubmittedcount;
	CuratorFramework cf;
	List<String> containers;
	boolean iscompleted;
	public TaskSchedulerReducerSubmitter(String currentexecutor, ReducerValues rv, String applicationid, String taskid2,
			long reducersubmittedcount, CuratorFramework cf, List<String> containers) {
		this.rv = rv;
		this.hp = currentexecutor;
		this.applicationid = applicationid;
		this.taskid = taskid2;
		this.reducersubmittedcount = reducersubmittedcount;
		this.cf = cf;
		this.containers = containers;
		iscompleted = false;
	}

	public String getTaskExecutorBalanced(long currentexecutor, List<String> taskexecutors) {

		return taskexecutors.get((int) (currentexecutor % taskexecutors.size()));
	}

	@SuppressWarnings("unchecked")
	@Override
	public Context call() throws Exception {
		List objects = new ArrayList<>();
		objects.add(rv);
		objects.add(applicationid);
		objects.add(taskid);
		log.debug("Submitting Reducer Task: "+objects);
		Utils.writeObject(hp, objects);
		return null;

	}

	@Override
	public void setHostPort(String hp) {
		this.hp = hp;
	}

	@Override
	public long getReducerSubmittedCount() {
		return this.reducersubmittedcount;
	}

	@Override
	public String getHostPort() {
		return this.hp;
	}

	@Override
	public CuratorFramework getCuratorFramework() {
		return cf;
	}

	@Override
	public List<String> getContainers() {
		return containers;
	}
}
