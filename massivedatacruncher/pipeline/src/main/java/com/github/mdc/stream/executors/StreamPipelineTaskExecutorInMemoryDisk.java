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

import com.github.mdc.common.HeartBeatTaskSchedulerStream;
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
			Cache cache, HeartBeatTaskSchedulerStream hbtss) throws Exception {
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
		return task.jobid + MDCConstants.HYPHEN
				+ task.stageid + MDCConstants.HYPHEN + task.taskid;
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
			os = new ByteArrayOutputStream();
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
		OutputStream os = resultstream.get(path);
		log.debug("Exiting MassiveDataStreamTaskExecutorInMemory.getIntermediateInputStreamFS");
		if (Objects.isNull(os)) {
			log.info("Unable to get Result Stream for path: " + path + " Fetching Remotely");
			return null;
		}
		else if (os instanceof ByteArrayOutputStream baos) {
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
		var path = rdf.jobid + MDCConstants.HYPHEN
				+ rdf.stageid + MDCConstants.HYPHEN + rdf.taskid;
		return (byte[]) cache.get(path);
	}

	@Override
	public void run() {
		starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskExecutorInMemory.call");
		var stageTasks = getStagesTask();
		var hdfsfilepath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL);
		var configuration = new Configuration();
		var timetakenseconds = 0.0;
		try (var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), configuration);) {

			endtime = System.currentTimeMillis();
			hbtss.pingOnce(task, Task.TaskStatus.SUBMITTED, new Long[]{starttime,endtime}, timetakenseconds, null);
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
							task.input[inputindex] = new SnappyInputStream(new ByteArrayInputStream(btarray));
						} else {
							RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
							task.input[inputindex] = new SnappyInputStream(new ByteArrayInputStream(rdf.data));
						}
					}
				}
			}
			hbtss.pingOnce(task, Task.TaskStatus.RUNNING, new Long[]{starttime,endtime}, timetakenseconds, null);
			log.debug("Running Stage " + task.jobid + " " + task.stageid + " " + jobstage);
			timetakenseconds = computeTasks(task, hdfs);
			completed = true;
			endtime = System.currentTimeMillis();
			hbtss.pingOnce(task, Task.TaskStatus.COMPLETED, new Long[]{starttime,endtime}, timetakenseconds, null);
			log.debug("Completed JobStage " + task.jobid + " " + task.stageid + " in " + timetakenseconds);
		} catch (Exception ex) {
			completed = true;
			log.error("Failed Stage: " + task.stageid, ex);
			try {
				var baos = new ByteArrayOutputStream();
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
				endtime = System.currentTimeMillis();
				hbtss.pingOnce(task, Task.TaskStatus.FAILED, new Long[]{starttime,endtime}, 0.0, new String(baos.toByteArray()));
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
		log.debug("Exiting MassiveDataStreamTaskExecutorInMemory.call");
	}

}
