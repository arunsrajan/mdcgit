package com.github.mdc.stream.executors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.ehcache.Cache;
import org.xerial.snappy.SnappyInputStream;

import com.github.mdc.common.ByteBufferPool;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.CloseableByteBufferOutputStream;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.PipelineConstants;
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
public sealed class StreamPipelineTaskExecutorInMemory extends StreamPipelineTaskExecutor permits StreamPipelineTaskExecutorInMemoryDisk  {
	private static Logger log = Logger.getLogger(StreamPipelineTaskExecutorInMemory.class);
	protected ConcurrentMap<String,OutputStream> resultstream = null;
	public double timetaken = 0.0;
	
	public StreamPipelineTaskExecutorInMemory(JobStage jobstage, 
			ConcurrentMap<String,OutputStream> resultstream, Cache cache) {
		super(jobstage, cache);
		this.resultstream = resultstream;
	}
	
	public OutputStream getIntermediateInputStreamRDF(RemoteDataFetch rdf) throws Exception {
		log.debug("Entered MassiveDataStreamTaskExecutorInMemory.getIntermediateInputStreamRDF");
		var path = (rdf.jobid + MDCConstants.HYPHEN +
				rdf.stageid + MDCConstants.HYPHEN +rdf.taskid
				);
		OutputStream os = resultstream.get(path);
		log.debug("Exiting MassiveDataStreamTaskExecutorInMemory.getIntermediateInputStreamFS");
		if(Objects.isNull(os)) {
			log.info("Unable to get Result Stream for path: "+path+" Fetching Remotely");
			return os;
		}
		else if(os instanceof ByteArrayOutputStream baos) {
			return os;
		}
		else {
			throw new UnsupportedOperationException("Unknown I/O operation");
		}
	}
	
	/**
	 * Get the HDFS file path using the job and stage id.
	 * @return
	 */
	@Override
	public String getIntermediateDataFSFilePath(Task task) {
		return (task.jobid + MDCConstants.HYPHEN +
				task.stageid + MDCConstants.HYPHEN +task.taskid
						);
	}
	
	
	/**
	 * Create a file in HDFS and return the stream.
	 * @param hdfs
	 * @return
	 * @throws Exception 
	 * @throws Exception
	 */
	@Override
	public OutputStream createIntermediateDataToFS(Task task) throws PipelineException {
		log.debug("Entered MassiveDataStreamTaskExecutorInMemory.createIntermediateDataToFS");
		try {
			var path = getIntermediateDataFSFilePath(task);
			OutputStream os;
			if(task.finalphase && task.saveresulttohdfs) {
				os = new CloseableByteBufferOutputStream(ByteBufferPool.get().borrowObject());
			}
			else {
				os = new ByteArrayOutputStream();
				resultstream.put(path, os);
			}
			log.debug("Exiting MassiveDataStreamTaskExecutorInMemory.createIntermediateDataToFS");
			return os;
		} catch (Exception e) {
			log.error(PipelineConstants.FILEIOERROR, e);
			throw new PipelineException(PipelineConstants.FILEIOERROR, e);
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
		log.debug("Exiting MassiveDataStreamTaskExecutorInMemory.getIntermediateInputStreamFS");
		OutputStream os = resultstream.get(path);
		if(Objects.isNull(os)) {
			throw new NullPointerException("Unable to get Result Stream for path: "+path);
		}else if(os instanceof ByteArrayOutputStream baos) {
			return new ByteArrayInputStream(baos.toByteArray());
		} else {
			throw new UnsupportedOperationException("Unknown I/O operation");
		}
		
	}


	@Override
	public void run() {
		starttime = System.currentTimeMillis();
		log.info("Entered MassiveDataStreamTaskExecutorInMemory.call for task "+task);
		String stageTasks = "";
		log.info("Init Stage tasks");
		var hdfsfilepath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL_DEFAULT);
		log.info("Obtaining Namenode URL "+hdfsfilepath);
		var configuration = new Configuration();
		try(var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), configuration);) {
			stageTasks = getStagesTask();
			log.info("Submitted Task " + task);
			log.info("Running Task " + task);
			if (task.input != null && task.parentremotedatafetch != null) {
				var numinputs = task.parentremotedatafetch.length;
				for (var inputindex = 0; inputindex<numinputs;inputindex++) {
					var input = task.parentremotedatafetch[inputindex];
					if(input != null) {
						var rdf = (RemoteDataFetch) input;
						var os = getIntermediateInputStreamRDF(rdf);
						if (os != null) {
							task.input[inputindex] = new SnappyInputStream(new ByteArrayInputStream(((ByteArrayOutputStream)os).toByteArray()));
						} else {
							RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
							task.input[inputindex] = new SnappyInputStream(new ByteArrayInputStream(rdf.data));
						}
					}
				}
			}
			if(!Objects.isNull(hbtss)) {
				endtime = System.currentTimeMillis();
				hbtss.pingOnce(task, Task.TaskStatus.RUNNING, new Long[]{starttime,endtime}, timetaken, null);
			}
			timetaken = computeTasks(task, hdfs);
			if(!Objects.isNull(hbtss)) {
				endtime = System.currentTimeMillis();
				hbtss.pingOnce(task, Task.TaskStatus.COMPLETED, new Long[]{starttime,endtime}, timetaken, null);
			}
			log.info("Completed Task: " + task);
		} catch (Exception ex) {
			log.error("Failed Stage " + stageTasks, ex);
			completed = true;
			log.error("Failed Stage: " + task.stageid, ex);
			if(!Objects.isNull(hbtss)) {
				try {
					var baos = new ByteArrayOutputStream();
					var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
					ex.printStackTrace(failuremessage);
					endtime = System.currentTimeMillis();
					hbtss.pingOnce(task, Task.TaskStatus.FAILED,new Long[]{starttime,endtime}, 0.0,
							new String(baos.toByteArray()));
				} catch (Exception e) {
					log.error("Message Send Failed for Task Failed: ", e);
				}
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
	}
	
}
