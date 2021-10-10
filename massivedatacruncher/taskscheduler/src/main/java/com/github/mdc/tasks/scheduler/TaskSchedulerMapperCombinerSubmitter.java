package com.github.mdc.tasks.scheduler;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.curator.framework.CuratorFramework;

import com.github.mdc.common.ApplicationTask;
import com.github.mdc.common.Context;
import com.github.mdc.common.HeartBeatTaskScheduler;
import com.github.mdc.common.TaskSchedulerMapperCombinerSubmitterMBean;

@SuppressWarnings("rawtypes")
public class TaskSchedulerMapperCombinerSubmitter extends TaskSchedulerMapperSubmitter
		implements TaskSchedulerMapperCombinerSubmitterMBean,Callable<Context> {
	Set<String> combinerclasses;
	TaskSchedulerMapperCombinerSubmitter(Object blockslocation, boolean mapper,
			Set<String> mapperclasses,Set<String> combinerclasses,
			CuratorFramework cf, List<String> containers,
			HeartBeatTaskScheduler hbts,ApplicationTask apptask) {
		super(blockslocation, mapper, mapperclasses, apptask, cf, containers,hbts);
		this.combinerclasses = combinerclasses;
	}
	@Override
	public Context call() throws Exception {
		var blockslocation = initializeobject(mapperclasses,combinerclasses);
		sendChunk(blockslocation);
		return null;
	}
	@Override
	public long getCombinerSubmittedCount() {
		return 0;
	}

}
