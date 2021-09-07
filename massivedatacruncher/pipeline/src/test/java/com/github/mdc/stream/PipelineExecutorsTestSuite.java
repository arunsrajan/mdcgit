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

import com.github.mdc.common.ByteArrayOutputStreamPool;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.CacheUtils;
import com.github.mdc.common.MDCCache;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.executors.MassiveDataStreamTaskDExecutorTest;
import com.github.mdc.stream.executors.MassiveDataStreamTaskExecutorInMemoryDiskTest;
import com.github.mdc.stream.executors.MassiveDataStreamTaskExecutorInMemoryTest;
import com.github.mdc.stream.executors.MassiveDataStreamTaskExecutorYarnTest;
import com.github.mdc.stream.executors.MassiveDataStreamTaskIgniteTest;
import com.github.mdc.stream.executors.MassiveDataStreamTaskJGroupsExecutorTest;
import com.github.mdc.stream.utils.PipelineConfigValidatorTest;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;

@RunWith(Suite.class)
@SuiteClasses({ 
	MassiveDataStreamTaskExecutorInMemoryDiskTest.class,
	MassiveDataPipelineTest.class,
	MassiveDataStreamTaskDExecutorTest.class,
	MassiveDataStreamTaskExecutorInMemoryTest.class,
	MassiveDataStreamTaskJGroupsExecutorTest.class,	
	PipelineConfigValidatorTest.class,	
	MassiveDataStreamTaskExecutorYarnTest.class,
	FileBlocksPartitionerTest.class,
	MassiveDataStreamTaskIgniteTest.class})
public class PipelineExecutorsTestSuite extends MDCPipelineTestsCommon{
	@BeforeClass
	public static void setUp() throws Throwable {
		System.setProperty("HADOOP_HOME", "E:\\DEVELOPMENT\\hadoop\\hadoop-3.2.1");
		Configuration conf = new Configuration();
		Utils.loadLog4JSystemPropertiesClassPath("mdctest.properties");
		ByteBufferPoolDirect.init(3);
		CacheUtils.initCache();
		ByteArrayOutputStreamPool.init(2);
		cache = (Cache<String, byte[]>) MDCCache.get();
		try(Socket sock = new Socket("localhost",9000);){}
		catch(Exception ex) {
			conf.set("fs.hdfs.impl.disable.cache", "false");
			conf.set("dfs.block.access.token.enable", "true");
			hdfsLocalCluster = new HdfsLocalCluster.Builder().setHdfsNamenodePort(namenodeport)
					.setHdfsNamenodeHttpPort(namenodehttpport).setHdfsTempDir("./target/embedded_hdfs")
					.setHdfsNumDatanodes(1).setHdfsEnablePermissions(false).setHdfsFormat(true)
					.setHdfsEnableRunningUserAsProxyUser(true).setHdfsConfig(conf).build();

			hdfsLocalCluster.start();
		}
		hdfs = FileSystem.newInstance(new URI(hdfsurl),
				conf);
		uploadfile(hdfs, airlinesample, airlinesample + csvfileextn);
		uploadfile(hdfs, airlinesampleintersection, airlinesampleintersection + csvfileextn);
		uploadfile(hdfs, airlinesampleunion1, airlinesampleunion1 + csvfileextn);
		uploadfile(hdfs, airlinesampleunion2, airlinesampleunion2 + csvfileextn);
		uploadfile(hdfs, githubevents, githubevents + jsonfileextn);
		Utils.loadLog4JSystemPropertiesClassPath("mdctest.properties");
	}
	public static void uploadfile(FileSystem hdfs, String dir, String filename) throws Throwable {
		InputStream is = MDCPipelineTestsCommon.class.getResourceAsStream(filename);
		String jobpath = dir;
		String filepath = jobpath + filename;
		Path jobpathurl = new Path(jobpath);
		if (!hdfs.exists(jobpathurl)) {
			hdfs.mkdirs(jobpathurl);
		}
		Path filepathurl = new Path(filepath);
		FSDataOutputStream fsdos = hdfs.create(filepathurl);
		IOUtils.copy(is, fsdos);
		fsdos.hflush();
		is.close();
		fsdos.close();
	}
	@AfterClass
	public static void closeResources() throws Throwable {
		if(!Objects.isNull(hdfsLocalCluster)) {
			hdfsLocalCluster.stop(true);
		}
		if(!Objects.isNull(hdfs)) {
			hdfs.close();
		}
		ByteBufferPoolDirect.get().close();
	}
}
