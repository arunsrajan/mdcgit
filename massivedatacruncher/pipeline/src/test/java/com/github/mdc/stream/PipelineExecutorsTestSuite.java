package com.github.mdc.stream;

import java.io.InputStream;
import java.net.Socket;
import java.net.URI;
import java.util.Objects;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ehcache.Cache;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.github.mdc.common.ByteBufferPool;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.CacheUtils;
import com.github.mdc.common.MDCCache;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorTest;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorInMemoryDiskTest;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorInMemoryTest;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorYarnTest;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorIgniteTest;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorJGroupsTest;
import com.github.mdc.stream.utils.PipelineConfigValidatorTest;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;

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
public class PipelineExecutorsTestSuite extends StreamPipelineTestCommon{
	
}
