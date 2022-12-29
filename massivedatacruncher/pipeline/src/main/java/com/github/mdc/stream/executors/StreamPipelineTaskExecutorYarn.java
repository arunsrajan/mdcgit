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

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.github.mdc.common.FileSystemSupport;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.PipelineConstants;
import com.github.mdc.common.RemoteDataFetch;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.Task;
import com.github.mdc.stream.PipelineException;

/**
 * 
 * @author Arun
 * The yarn container task executor
 */
public final class StreamPipelineTaskExecutorYarn extends StreamPipelineTaskExecutor {
	private static final Log log = LogFactory.getLog(StreamPipelineTaskExecutorYarn.class);
	private String hdfsnn;

	public StreamPipelineTaskExecutorYarn(String hdfsnn, JobStage jobstage) {
		super(jobstage, null);
		this.hdfsnn = hdfsnn;
	}


	/**
	 * Prepare the HDFS file path given task object.
	 * 
	 * @return
	 */
	public String getIntermediateDataFSFilePath(Task task) {
		return MDCConstants.FORWARD_SLASH + FileSystemSupport.MDS + MDCConstants.FORWARD_SLASH + jobstage.getJobid()
				+ MDCConstants.FORWARD_SLASH + task.taskid;
	}

	/**
	 * Create a file in HDFS and return the stream.
	 * 
	 * @param hdfs
	 * @return
	 * @throws Exception
	 */
	public OutputStream createIntermediateDataToFS(Task task, int numbytes) throws PipelineException {
		log.debug("Entered StreamPipelineTaskExecutorYarn.createIntermediateDataToFS");
		try {
			var path = getIntermediateDataFSFilePath(task);
			var hdfspath = new Path(path);
			log.debug("Exiting StreamPipelineTaskExecutorYarn.createIntermediateDataToFS");
			return hdfs.create(hdfspath, false);
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		}
	}

	/**
	 * The runnable method executes the streaming api parallely.
	 */
	@Override
	public Boolean call() {
		try (var hdfs = FileSystem.newInstance(new URI(hdfsnn), new Configuration());) {
			this.hdfs = hdfs;
			var output = new ArrayList<>();

			if (task.input != null && task.parentremotedatafetch != null) {
				var numinputs = task.parentremotedatafetch.length;
				for (var inputindex = 0; inputindex < numinputs; inputindex++) {
					var input = task.parentremotedatafetch[inputindex];
					if (input != null) {
						var rdf = (RemoteDataFetch) input;
						//Intermediate data fetch from HDFS streaming API.
						task.input[inputindex] = RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(rdf.getJobid(),
								rdf.getTaskid(), hdfs);
					}
				}
			}
			//Join transformation operation of map reduce stream pipelining API.
			double timetakenseconds = computeTasks(task, hdfs);
			output.clear();
			completed = true;
		} catch (Exception ex) {
			log.error("Stage " + task.jobid + MDCConstants.SINGLESPACE + task.stageid + " failed, See cause below \n",
					ex);
		}
		return completed;
	}


}
