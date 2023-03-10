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

import com.github.mdc.stream.utils.FileBlocksPartitionerHDFSMultipleNodesTest;
import com.github.mdc.stream.utils.FileBlocksPartitionerHDFSTest;
import com.github.mdc.tasks.executor.MassiveDataCruncherMRApiTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({ StreamPipelineTestSuite2.class, StreamPipelineTest.class, StreamPipelineContinuedTest.class,
		StreamPipelineFoldByKeyKeyByTest.class, StreamPipelineTransformationsCollectTest.class,
		StreamPipelineTransformationsNullTest.class, StreamPipelineDepth2Test.class, StreamPipelineDepth31Test.class,
		StreamPipelineDepth32Test.class, StreamPipelineDepth32ContinuedTest.class, StreamPipelineDepth33Test.class,
		StreamPipelineDepth34Test.class, StreamPipelineUtilsTest.class, StreamPipelineFunctionsTest.class,
		StreamPipelineCoalesceTest.class, StreamPipelineJsonTest.class, StreamPipelineTransformationFunctionsTest.class,
		HDFSBlockUtilsTest.class, StreamPipelineStatisticsTest.class, MassiveDataCruncherMRApiTest.class,
		StreamPipelineSqlTest.class,
		FileBlocksPartitionerHDFSMultipleNodesTest.class,FileBlocksPartitionerHDFSTest.class})
public class StreamPipelineTestSuite{}
