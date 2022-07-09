package com.github.mdc.tasks.scheduler;

import org.junit.BeforeClass;

import com.github.mdc.common.Utils;

public class TaskSchedulerCommon {

	@BeforeClass
	public static void init() throws Exception {
		Utils.loadLog4JSystemPropertiesClassPath("mdctest.properties");
	}


}
