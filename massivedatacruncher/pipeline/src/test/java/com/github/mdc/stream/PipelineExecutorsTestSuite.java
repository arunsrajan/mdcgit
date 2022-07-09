package com.github.mdc.stream;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.github.mdc.stream.executors.StreamPipelineTaskExecutorTest;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorInMemoryDiskTest;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorInMemoryTest;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorYarnTest;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorIgniteTest;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorJGroupsTest;
import com.github.mdc.stream.utils.PipelineConfigValidatorTest;

@RunWith(Suite.class)
@SuiteClasses({
		StreamPipelineTaskExecutorInMemoryDiskTest.class,
		StreamPipelineTest.class,
		StreamPipelineTaskExecutorTest.class,
		StreamPipelineTaskExecutorInMemoryTest.class,
		StreamPipelineTaskExecutorJGroupsTest.class,
		PipelineConfigValidatorTest.class,
		StreamPipelineTaskExecutorYarnTest.class,
		FileBlocksPartitionerTest.class,
		StreamPipelineTaskExecutorIgniteTest.class})
public class PipelineExecutorsTestSuite extends StreamPipelineTestCommon {

}
