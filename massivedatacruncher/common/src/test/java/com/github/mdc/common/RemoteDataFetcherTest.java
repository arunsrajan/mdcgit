package com.github.mdc.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.Socket;
import java.net.URI;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.FileSystemSupport;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.RemoteDataFetcherException;
import com.github.sakserv.minicluster.impl.HdfsLocalCluster;

public class RemoteDataFetcherTest {

	static HdfsLocalCluster hdfsLocalCluster;
	static int namenodeport = 9000;
	static int namenodehttpport = 60070;
	@BeforeClass
	public static void setUpHdfs() throws Exception {
		System.setProperty("HADOOP_HOME", "E:\\DEVELOPMENT\\hadoop\\hadoop-3.2.1");
		try(Socket sock = new Socket("localhost",9000);){}
		catch(Exception ex) {
			Configuration conf = new Configuration();
			conf.set("fs.hdfs.impl.disable.cache", "false");
			conf.set("dfs.block.access.token.enable", "true");
			hdfsLocalCluster = new HdfsLocalCluster.Builder().setHdfsNamenodePort(namenodeport)
					.setHdfsNamenodeHttpPort(namenodehttpport).setHdfsTempDir("./target/embedded_hdfs")
					.setHdfsNumDatanodes(1).setHdfsEnablePermissions(false).setHdfsFormat(true)
					.setHdfsEnableRunningUserAsProxyUser(true).setHdfsConfig(conf).build();

			hdfsLocalCluster.start();
		}
		
		Utils.loadLog4JSystemPropertiesClassPath(MDCConstants.MDC_TEST_PROPERTIES);
		System.setProperty(MDCConstants.HDFSNAMENODEURL, MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testWriteReadInterMediateOutputToFromDFS() throws Exception {
		DataCruncherContext<String,String> ctx = new DataCruncherContext<>();
		String testdata = "TestData";
		ctx.put(testdata, testdata);
		String filename = "TestFile.dat";
		String job = MDCConstants.JOB+MDCConstants.HYPHEN+System.currentTimeMillis();
		RemoteDataFetcher.writerIntermediatePhaseOutputToDFS(ctx, job, filename);
		Set<String> keys = (Set<String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, true);
		assertNotNull(keys);
		assertTrue(keys.size()==1);
		ctx = null;
		ctx = (DataCruncherContext<String, String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, false);
		assertNotNull(ctx);
		assertTrue(ctx.keys().size()==1);
		assertTrue(ctx.get(testdata).contains(testdata));
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testWriteReadInterMediateOutputToFromDFSReadException(){
		try {
			DataCruncherContext<String,String> ctx = new DataCruncherContext<>();
			String testdata = "TestData";
			ctx.put(testdata, testdata);
			String filename = "TestFile1.dat";
			String unavailablefilename = "TestFile2.dat";
			String job = MDCConstants.JOB+MDCConstants.HYPHEN+System.currentTimeMillis();
			RemoteDataFetcher.writerIntermediatePhaseOutputToDFS(ctx, job, filename);
			Set<String> keys = (Set<String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, true);
			assertNotNull(keys);
			assertTrue(keys.size()==1);
			ctx = null;
			ctx = (DataCruncherContext<String, String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, unavailablefilename, false);
			assertNotNull(ctx);
			assertTrue(ctx.keys().size()==1);
			assertTrue(ctx.get(testdata).contains(testdata));
		}
		catch(Exception ex) {
			assertEquals(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR,ex.getMessage());
		}
	}
	@SuppressWarnings("unchecked")
	@Test
	public void testWriteReadInterMediateOutputToFromDFSReadKeysException(){
		try {
			DataCruncherContext<String,String> ctx = new DataCruncherContext<>();
			String testdata = "TestData";
			ctx.put(testdata, testdata);
			String filename = "TestFile1.dat";
			String unavailablefilename = "TestFile2.dat";
			String job = MDCConstants.JOB+MDCConstants.HYPHEN+System.currentTimeMillis();
			RemoteDataFetcher.writerIntermediatePhaseOutputToDFS(ctx, job, filename);
			Set<String> keys = (Set<String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, unavailablefilename, true);
			assertNotNull(keys);
			assertTrue(keys.size()==1);
			ctx = null;
			ctx = (DataCruncherContext<String, String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, false);
			assertNotNull(ctx);
			assertTrue(ctx.keys().size()==1);
			assertTrue(ctx.get(testdata).contains(testdata));
		}
		catch(Exception ex) {
			assertEquals(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR,ex.getMessage());
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testCreateFileMR() throws Exception{
			String filename = "TestFile1.dat";
			String job = MDCConstants.JOB+MDCConstants.HYPHEN+System.currentTimeMillis();
			Configuration configuration = new Configuration();
			configuration.set(MDCConstants.HDFS_DEFAULTFS, MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL));
			configuration.set(MDCConstants.HDFS_IMPL, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			configuration.set(MDCConstants.HDFS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName());
			String jobpath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL) + MDCConstants.BACKWARD_SLASH
					+ FileSystemSupport.MDS + MDCConstants.BACKWARD_SLASH + job;
			String filepath = jobpath + MDCConstants.BACKWARD_SLASH + filename;
			// Create folders if not already created.
			Path filepathurl = new Path(filepath);
			FileSystem hdfs = FileSystem.newInstance(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
					configuration);
			String testdata = "TestData";
			DataCruncherContext<String, String> ctx = new DataCruncherContext<>();
			ctx.put(testdata, testdata);
			RemoteDataFetcher.createFileMR(hdfs, filepathurl, ctx);
			
			Set<String> keys = (Set<String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, true);
			assertNotNull(keys);
			assertTrue(keys.size()==1);
			ctx = null;
			ctx = (DataCruncherContext<String, String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, false);
			assertNotNull(ctx);
			assertTrue(ctx.keys().size()==1);
			assertTrue(ctx.get(testdata).contains(testdata));
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCreateFileMRException() {
		try {
			String filename = "TestFile1.dat";
			String job = MDCConstants.JOB+MDCConstants.HYPHEN+System.currentTimeMillis();
			Configuration configuration = new Configuration();
			configuration.set(MDCConstants.HDFS_DEFAULTFS, MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL));
			configuration.set(MDCConstants.HDFS_IMPL, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			configuration.set(MDCConstants.HDFS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName());
			MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL);
			FileSystem hdfs = FileSystem.newInstance(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
					configuration);
			String testdata = "TestData";
			DataCruncherContext ctx = new DataCruncherContext();
			ctx.put(testdata, testdata);
			RemoteDataFetcher.createFileMR(hdfs, null, ctx);
			
			Set<String> keys = (Set<String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, true);
			assertNotNull(keys);
			assertTrue(keys.size()==1);
			ctx = null;
			ctx = (DataCruncherContext<String, String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, false);
			assertNotNull(ctx);
			assertTrue(ctx.keys().size()==1);
			assertTrue(ctx.get(testdata).contains(testdata));
		}
		catch(Exception ex) {
			assertEquals(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR,ex.getMessage());
		}
	}
	
	@Test
	public void testWriteReadYarnInterMediateOutputToFromDFS() throws Exception {
		String filename = "Test.dat";
		String dirtowrite = "/Dir";
		RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(new JobStage(), dirtowrite, filename);
		JobStage js = (JobStage) RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS(dirtowrite, filename);
		assertNotNull(js);	
	}
	
	@Test
	public void testWriteReadYarnInterMediateOutputToFromDFSReadException(){
		try {
			String filename = "Test.dat";
			String dirtowrite = "/Dir";
			String dirtoread = "/UnknownDir";
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(new JobStage(), dirtowrite, filename);
			RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS(dirtoread, filename);
		}
		catch(Exception ex) {
			assertEquals(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR,ex.getMessage());
		}
	}
	@Test
	public void testCreateFile() throws Exception{
			String filename = "TestFile3.dat";
			String dir = MDCConstants.JOB+MDCConstants.HYPHEN+System.currentTimeMillis();
			Configuration configuration = new Configuration();
			configuration.set(MDCConstants.HDFS_DEFAULTFS, MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL));
			configuration.set(MDCConstants.HDFS_IMPL, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			configuration.set(MDCConstants.HDFS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName());
			String jobpath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL) + MDCConstants.BACKWARD_SLASH
					+ FileSystemSupport.MDS + MDCConstants.BACKWARD_SLASH + dir;
			String filepath = jobpath + MDCConstants.BACKWARD_SLASH + filename;
			Path filepathurl = new Path(filepath);
			FileSystem hdfs = FileSystem.newInstance(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
					configuration);
			JobStage js = new JobStage();
			RemoteDataFetcher.createFile(hdfs, filepathurl, js);
			
			js = (JobStage) RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS(dir, filename);
			assertNotNull(js);	
	}
	@Test
	public void testCreateFileException() {
		try {
			String filename = "TestFile1.dat";
			String dir = MDCConstants.JOB+MDCConstants.HYPHEN+System.currentTimeMillis();
			Configuration configuration = new Configuration();
			configuration.set(MDCConstants.HDFS_DEFAULTFS, MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL));
			configuration.set(MDCConstants.HDFS_IMPL, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			configuration.set(MDCConstants.HDFS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName());
			String jobpath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL) + MDCConstants.BACKWARD_SLASH
					+ FileSystemSupport.MDS + MDCConstants.BACKWARD_SLASH + dir;
			String filepath = jobpath + MDCConstants.BACKWARD_SLASH + filename;
			Path filepathurl = new Path(filepath);
			JobStage js = new JobStage();
			RemoteDataFetcher.createFile(null, filepathurl, js);
		}
		catch(Exception ex) {
			assertEquals(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR,ex.getMessage());
		}
	}
	
	
	@Test
	public void testdeleteFolder() throws Exception {
		String filename = "TestFiledelete1.dat";
		String dir = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		Configuration configuration = new Configuration();
		configuration.set(MDCConstants.HDFS_DEFAULTFS,
				MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL));
		configuration.set(MDCConstants.HDFS_IMPL, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		configuration.set(MDCConstants.HDFS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName());
		String jobpath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL) + MDCConstants.BACKWARD_SLASH
				+ FileSystemSupport.MDS + MDCConstants.BACKWARD_SLASH + dir;
		String filepath = jobpath + MDCConstants.BACKWARD_SLASH + filename;
		Path filepathurl = new Path(filepath);
		FileSystem hdfs = FileSystem
				.newInstance(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)), configuration);
		JobStage js = new JobStage();
		RemoteDataFetcher.createFile(hdfs, filepathurl, js);
		assertTrue(hdfs.exists(filepathurl));
		RemoteDataFetcher.deleteIntermediatePhaseOutputFromDFS(dir);
		assertFalse(hdfs.exists(filepathurl));

	}
	
	@Test
	public void testdeleteFolderException() {
		try {
			String filename = "TestFiledelete2.dat";
			String dir = MDCConstants.JOB+MDCConstants.HYPHEN+System.currentTimeMillis();
			Configuration configuration = new Configuration();
			configuration.set(MDCConstants.HDFS_DEFAULTFS, MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL));
			configuration.set(MDCConstants.HDFS_IMPL, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			configuration.set(MDCConstants.HDFS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName());
			String jobpath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL) + MDCConstants.BACKWARD_SLASH
					+ FileSystemSupport.MDS + MDCConstants.BACKWARD_SLASH + dir;
			String filepath = jobpath + MDCConstants.BACKWARD_SLASH + filename;
			Path filepathurl = new Path(filepath);
			FileSystem hdfs = FileSystem.newInstance(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
					configuration);
			JobStage js = new JobStage();
			RemoteDataFetcher.createFile(hdfs, filepathurl, js);
			assertTrue(hdfs.exists(filepathurl));
			RemoteDataFetcher.deleteIntermediatePhaseOutputFromDFS(jobpath);
		}
		catch(Exception ex) {
			assertEquals(RemoteDataFetcherException.INTERMEDIATEPHASEDELETEERROR,ex.getMessage());
		}
	}
	@AfterClass
	public static void closeHdfs() throws Exception {
		if(!Objects.isNull(hdfsLocalCluster)) {
			hdfsLocalCluster.stop(true);
		}
	}
}
