package com.github.mdc.tasks.scheduler;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.curator.framework.CuratorFramework;

import com.github.mdc.common.ApplicationTask;
import com.github.mdc.common.Context;
import com.github.mdc.common.HeartBeatTaskScheduler;
import com.github.mdc.common.MassiveDataTaskSchedulerThreadMapperCombinerMBean;

@SuppressWarnings("rawtypes")
public class MassiveDataTaskSchedulerThreadMapperCombiner extends MassiveDataTaskSchedulerThreadMapper
		implements MassiveDataTaskSchedulerThreadMapperCombinerMBean,Callable<Context> {
	Set<String> combinerclasses;
	MassiveDataTaskSchedulerThreadMapperCombiner(Object blockslocation, boolean mapper,
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
