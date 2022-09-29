package com.github.mdc.stream.executors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.ehcache.Cache;

import com.github.mdc.common.ByteBufferInputStream;
import com.github.mdc.common.ByteBufferOutputStream;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.PipelineConstants;
import com.github.mdc.common.RemoteDataFetch;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.Task;
import com.github.mdc.stream.PipelineException;

import static java.util.Objects.*;

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
		var path = (rdf.getJobid() + MDCConstants.HYPHEN +
				rdf.getStageid() + MDCConstants.HYPHEN +rdf.getTaskid()
				);
		OutputStream os = resultstream.get(path);
		log.debug("Exiting MassiveDataStreamTaskExecutorInMemory.getIntermediateInputStreamFS");
		if(isNull(os)) {
			return null;
		}
		else if(os instanceof ByteBufferOutputStream baos) {
			return baos;
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
		return task.jobid + MDCConstants.HYPHEN +
				task.stageid + MDCConstants.HYPHEN +task.taskid
						;
	}
	
	
	/**
	 * Create a file in HDFS and return the stream.
	 * @param hdfs
	 * @return
	 * @throws Exception 
	 * @throws Exception
	 */
	@Override
	public OutputStream createIntermediateDataToFS(Task task,int buffersize) throws PipelineException {
		log.debug("Entered MassiveDataStreamTaskExecutorInMemory.createIntermediateDataToFS");
		try {
			var path = getIntermediateDataFSFilePath(task);
			OutputStream os;
			os = new ByteBufferOutputStream(ByteBufferPoolDirect.get(buffersize));
			resultstream.put(path, os);
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
		if(os instanceof ByteBufferOutputStream baos) {
			return new ByteBufferInputStream(baos.get());
		} else {
			throw new UnsupportedOperationException("Unknown I/O operation");
		}
		
	}


	@Override
	public Boolean call() {
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
							ByteBufferOutputStream bbos = (ByteBufferOutputStream) os;							
							task.input[inputindex] = new ByteBufferInputStream(bbos.get().rewind());
						} else {
							RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
							task.input[inputindex] = new ByteArrayInputStream(rdf.getData());
						}
					}
				}
			}
			timetaken = computeTasks(task, hdfs);
			log.info("Completed Task: " + task);
			completed = true;
		} catch (Exception ex) {
			log.error("Failed Stage " + stageTasks, ex);
			completed = false;
			log.error("Failed Stage: " + task.stageid, ex);
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
		return completed;
	}
	
}
