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

import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskID;

import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.RemoteDataFetch;
import com.github.mdc.common.RemoteDataFetcher;

/**
 * 
 * @author Arun
 * Mesos task executor.
 */
public final class StreamPipelineTaskExecutorMesos extends StreamPipelineTaskExecutor {
	private ExecutorDriver driver;
	TaskID taskid;
	private static Logger log = Logger.getLogger(StreamPipelineTaskExecutorMesos.class);

	public StreamPipelineTaskExecutorMesos(JobStage jobstage, ExecutorDriver driver,
			TaskID taskid) {
		super(jobstage, null);
		this.driver = driver;
		this.taskid = taskid;
	}

	/**
	 * Execute the tasks via run method.
	 */
	@Override
	public Boolean call() {

		log.debug("Entered MassiveDataStreamTaskExecutorMesos.run");
		var configuration = new Configuration();
		;
		var hdfsfilepath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL, MDCConstants.HDFSNAMENODEURL);
		try (var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), configuration);) {


			var output = new ArrayList<>();
			var status = Protos.TaskStatus.newBuilder().setTaskId(taskid)
					.setState(Protos.TaskState.TASK_RUNNING).build();
			driver.sendStatusUpdate(status);

			if (task.input != null && task.parentremotedatafetch != null) {
				var numinputs = task.parentremotedatafetch.length;
				for (var inputindex = 0; inputindex < numinputs; inputindex++) {
					var input = task.parentremotedatafetch[inputindex];
					if (input != null) {
						var rdf = (RemoteDataFetch) input;
						task.input[inputindex] = RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(rdf.getJobid(),
								getIntermediateDataFSFilePath(rdf.getJobid(), rdf.getStageid(), rdf.getTaskid()), hdfs);
					}
				}
			}

			double timetakenseconds = computeTasks(task, hdfs);
			log.info("Absolute Time Taken: " + timetakenseconds + " Seconds");
			status = Protos.TaskStatus.newBuilder().setTaskId(taskid).setState(Protos.TaskState.TASK_FINISHED).build();
			driver.sendStatusUpdate(status);
			output.clear();
		} catch (Exception ex) {
			log.error("Failed Stage " + task.jobid + MDCConstants.SINGLESPACE + task.stageid + " failed: ",
					ex);
			Protos.TaskStatus status = Protos.TaskStatus.newBuilder().setTaskId(taskid).setState(Protos.TaskState.TASK_FAILED).build();
			driver.sendStatusUpdate(status);
		}
		log.debug("Exiting MassiveDataStreamTaskExecutorMesos.run");
		return completed;
	}


}
