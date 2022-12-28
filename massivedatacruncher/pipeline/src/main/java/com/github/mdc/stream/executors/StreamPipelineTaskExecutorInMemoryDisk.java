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
package com.github.mdc.stream.executors;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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

/**
 * 
 * @author Arun
 * Task executors thread for standalone task executors daemon.  
 */
@SuppressWarnings("rawtypes")
public final class StreamPipelineTaskExecutorInMemoryDisk extends StreamPipelineTaskExecutorInMemory {
	private static Logger log = Logger.getLogger(StreamPipelineTaskExecutorInMemoryDisk.class);

	public StreamPipelineTaskExecutorInMemoryDisk(JobStage jobstage,
			ConcurrentMap<String, OutputStream> resultstream,
			Cache cache) throws Exception {
		super(jobstage, resultstream, cache);
		iscacheable = true;
	}


	/**
	 * Get the HDFS file path using the job and stage id.
	 * @return
	 */
	@Override
	public String getIntermediateDataFSFilePath(Task task) {
		return task.jobid + MDCConstants.HYPHEN
				+ task.stageid + MDCConstants.HYPHEN + task.taskid;
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
		log.debug("Entered StreamPipelineTaskExecutorInMemoryDisk.createIntermediateDataToFS");
		try {
			var path = getIntermediateDataFSFilePath(task);
			OutputStream os;
			os = new ByteBufferOutputStream(ByteBufferPoolDirect.get(buffersize));
			resultstream.put(path, os);
			log.debug("Exiting StreamPipelineTaskExecutorInMemoryDisk.createIntermediateDataToFS");
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
		log.debug("Entered StreamPipelineTaskExecutorInMemoryDisk.getIntermediateInputStreamFS");
		var path = getIntermediateDataFSFilePath(task);
		log.debug("Exiting StreamPipelineTaskExecutorInMemoryDisk.getIntermediateInputStreamFS");
		OutputStream os = resultstream.get(path);
		if(Objects.isNull(os)) {
			throw new NullPointerException("Unable to get Result Stream for path: "+path);
		}else if(os instanceof ByteBufferOutputStream baos) {
			return new ByteBufferInputStream(baos.get());
		} else {
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
		log.debug("Entered StreamPipelineTaskExecutorInMemoryDisk.getIntermediateInputStreamRDF");
		var path = rdf.getJobid() + MDCConstants.HYPHEN
				+ rdf.getStageid() + MDCConstants.HYPHEN + rdf.getTaskid();
		return (byte[]) cache.get(path);
	}

	@Override
	public Boolean call() {
		starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutorInMemoryDisk.call");
		var stageTasks = getStagesTask();
		var hdfsfilepath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL_DEFAULT);
		var configuration = new Configuration();
		var timetakenseconds = 0.0;
		try (var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), configuration);) {

			endtime = System.currentTimeMillis();
			log.debug("Submitted JobStage " + task.jobid + " " + task.stageid + " " + jobstage);
			log.debug("Running Stage " + stageTasks);
			if (task.input != null && task.parentremotedatafetch != null) {
				var numinputs = task.parentremotedatafetch.length;
				for (var inputindex = 0; inputindex < numinputs; inputindex++) {
					var input = task.parentremotedatafetch[inputindex];
					if (input != null) {
						var rdf = (RemoteDataFetch) input;
						var btarray = getCachedRDF(rdf);
						if (btarray != null) {
							task.input[inputindex] =new ByteArrayInputStream(btarray);
						} else {
							RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
							task.input[inputindex] = new ByteArrayInputStream(rdf.getData());
						}
					}
				}
			}
			log.debug("Running Stage " + task.jobid + " " + task.stageid + " " + jobstage);
			timetakenseconds = computeTasks(task, hdfs);
			completed = true;
			endtime = System.currentTimeMillis();
			log.debug("Completed JobStage " + task.jobid + " " + task.stageid + " in " + timetakenseconds);
		} catch (Exception ex) {
			completed = false;
			log.error("Failed Stage: " + task.stageid, ex);
			try {
				var baos = new ByteArrayOutputStream();
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
				endtime = System.currentTimeMillis();
			} catch (Exception e) {
				log.error("Message Send Failed for Task Failed: ", e);
			}
		} finally {
			if (!Objects.isNull(hdfs)) {
				try {
					hdfs.close();
				} catch (Exception e) {
					log.error("HDFS client close error: ", e);
				}
			}
		}
		log.debug("Exiting StreamPipelineTaskExecutorInMemoryDisk.call");
		return completed;
	}

}
