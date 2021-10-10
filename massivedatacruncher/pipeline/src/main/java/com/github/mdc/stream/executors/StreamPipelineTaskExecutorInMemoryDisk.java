package com.github.mdc.stream.executors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.ehcache.Cache;
import org.xerial.snappy.SnappyInputStream;

import com.github.mdc.common.ByteBufferPool;
import com.github.mdc.common.CloseableByteBufferOutputStream;
import com.github.mdc.common.HeartBeatTaskSchedulerStream;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.MassiveDataPipelineConstants;
import com.github.mdc.common.RemoteDataFetch;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.Task;
import com.github.mdc.stream.PipelineException;

/**
 * 
 * @author Arun
 * Task executors thread for standalone task executors daemon.  
 */
@SuppressWarnings("rawtypes")
public final class StreamPipelineTaskExecutorInMemoryDisk extends StreamPipelineTaskExecutorInMemory {
	private static Logger log = Logger.getLogger(StreamPipelineTaskExecutorInMemoryDisk.class);
	
	public StreamPipelineTaskExecutorInMemoryDisk(JobStage jobstage,
			ConcurrentMap<String,OutputStream> resultstream,
			Cache cache,HeartBeatTaskSchedulerStream hbtss) throws Exception {
		super(jobstage, resultstream, cache);
		iscacheable = true;
		this.hbtss = hbtss;
	}
	
	
	
	/**
	 * Get the HDFS file path using the job and stage id.
	 * @return
	 */
	@Override
	public String getIntermediateDataFSFilePath(Task task) {
		return (task.jobid + MDCConstants.HYPHEN +
				task.stageid + MDCConstants.HYPHEN +task.taskid
						+ MDCConstants.DATAFILEEXTN);
	}
	
	
	/**
	 * Create a file in HDFS and return the stream.
	 * @param hdfs
	 * @return
	 * @throws FileNotFoundException 
	 * @throws Exception
	 */
	@Override
	public OutputStream createIntermediateDataToFS(Task task) throws PipelineException {
		log.debug("Entered MassiveDataStreamTaskExecutorInMemory.createIntermediateDataToFS");
		try {
			OutputStream os;
			os = new CloseableByteBufferOutputStream(ByteBufferPool.get().borrowObject());
			log.debug("Exiting MassiveDataStreamTaskExecutorInMemory.createIntermediateDataToFS");
			return os;
		} catch (Exception e) {
			log.error(MassiveDataPipelineConstants.FILEIOERROR, e);
			throw new PipelineException(MassiveDataPipelineConstants.FILEIOERROR, e);
		}
	}

	

	/**
	 * Open the already existing file using the job and stageid.
	 * @param hdfs
	 * @return
	 * @throws Exception 
	 */
	@Override
	public InputStream getIntermediateInputStreamFS(Task task) throws Exception {
		log.debug("Entered MassiveDataStreamTaskExecutorInMemory.getIntermediateInputStreamFS");
		var path = getIntermediateDataFSFilePath(task);
		OutputStream os = resultstream.get(path);
		log.debug("Exiting MassiveDataStreamTaskExecutorInMemory.getIntermediateInputStreamFS");
		if(Objects.isNull(os)) {
			log.info("Unable to get Result Stream for path: "+path+" Fetching Remotely");
			return null;
		}
		else if(os instanceof ByteArrayOutputStream baos) {
			return new ByteArrayInputStream((byte[]) cache.get(path));
		}
		else {
			throw new UnsupportedOperationException("Unknown I/O operation");
		}
	}

	/**
	 * Open the already existing data from memory.
	 * @param rdf
	 * @return
	 * @throws Exception 
	 */
	public byte[] getCachedRDF(RemoteDataFetch rdf) throws Exception {
		log.debug("Entered MassiveDataStreamTaskExecutorInMemory.getIntermediateInputStreamRDF");
		var path = (rdf.jobid + MDCConstants.HYPHEN +
				rdf.stageid + MDCConstants.HYPHEN +rdf.taskid
				+ MDCConstants.DATAFILEEXTN);
		return (byte[]) cache.get(path);
	}
	@Override
	public StreamPipelineTaskExecutorInMemoryDisk call() {
		log.debug("Entered MassiveDataStreamTaskExecutorInMemory.call");
		var stageTasks = getStagesTask();
		var hdfsfilepath = MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HDFSNN);
		var configuration = new Configuration();
		var timetakenseconds = 0.0;
		try(var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), configuration);) {
			
			hbtss.setTimetakenseconds(timetakenseconds);
			hbtss.pingOnce(task.stageid, task.taskid, task.hostport, Task.TaskStatus.SUBMITTED, timetakenseconds, null);
			log.debug("Submitted JobStage " + task.jobid+" "+task.stageid+" "+jobstage);
			log.debug("Running Stage " + stageTasks);
			hbtss.setTaskstatus(Task.TaskStatus.RUNNING);
			if (task.input != null && task.parentremotedatafetch != null) {
				var numinputs = task.parentremotedatafetch.length;
				for (var inputindex = 0; inputindex<numinputs;inputindex++) {
					var input = task.parentremotedatafetch[inputindex];
					if (input != null) {
						var rdf = (RemoteDataFetch) input;
						var btarray = getCachedRDF(rdf);
						if (btarray != null) {
							task.input[inputindex] = new SnappyInputStream(new ByteArrayInputStream(btarray));
						} else {
							RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
							task.input[inputindex] = new SnappyInputStream(new ByteArrayInputStream(rdf.data));
						}
					}
				}
			}
			log.debug("Running Stage " + task.jobid+" "+task.stageid+" "+jobstage);
			timetakenseconds = computeTasks(task, hdfs);
			completed = true;
			hbtss.setTimetakenseconds(timetakenseconds);
			hbtss.pingOnce(task.stageid, task.taskid, task.hostport, Task.TaskStatus.COMPLETED, timetakenseconds, null);
			log.debug("Completed JobStage " + task.jobid+" "+task.stageid+" in "+timetakenseconds);
		} catch (Exception ex) {
			completed = true;
			log.error("Failed Stage: "+task.stageid, ex);
			try {
				var baos = new ByteArrayOutputStream();
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
				hbtss.pingOnce(task.stageid, task.taskid, task.hostport, Task.TaskStatus.FAILED, 0.0, new String(baos.toByteArray()));
			} catch (Exception e) {
				log.error("Message Send Failed for Task Failed: ",e);
			}
		} finally {
			if(!Objects.isNull(hdfs)) {
				try {
					hdfs.close();
				} catch (Exception e) {
					log.error("HDFS client close error: ", e);
				}
			}
		}
		log.debug("Exiting MassiveDataStreamTaskExecutorInMemory.call");
		return this;
	}
	
}
