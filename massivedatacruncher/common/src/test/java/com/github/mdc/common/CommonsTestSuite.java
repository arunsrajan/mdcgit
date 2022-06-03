package com.github.mdc.common;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ HeartBeatServerStreamTest.class, HeartBeatServerTest.class, HeartBeatTaskSchedulerStreamTest.class,
		HeartBeatTaskSchedulerTest.class, ResourcesTest.class, ServerUtilsTest.class, ZkChunkPropTest.class, HeartBeatObservableTest.class,
		RemoteDataFetcherTest.class, DataCruncherContextTest.class})
public class CommonsTestSuite {

}
