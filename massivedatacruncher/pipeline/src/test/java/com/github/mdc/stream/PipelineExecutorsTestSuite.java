/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.mdc.stream;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorIgniteTest;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorInMemoryDiskTest;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorInMemoryTest;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorJGroupsTest;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorTest;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorYarnTest;
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
public class PipelineExecutorsTestSuite{

}
