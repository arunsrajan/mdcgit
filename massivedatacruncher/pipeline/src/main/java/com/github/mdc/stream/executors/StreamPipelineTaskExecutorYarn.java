package com.github.mdc.stream.executors;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Objects;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.xerial.snappy.SnappyInputStream;

import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.RemoteDataFetcher;
/**
 * 
 * @author Arun
 * The yarn container task executor
 */
public final class StreamPipelineTaskExecutorYarn extends StreamPipelineTaskExecutor {
	private static final Log log = LogFactory.getLog(StreamPipelineTaskExecutorYarn.class);
	private String hdfsnn;
	public StreamPipelineTaskExecutorYarn(String hdfsnn,JobStage jobstage) {
		super(jobstage, null);
		this.hdfsnn = hdfsnn;
	}
	/**
	 * The runnable method executes the streaming api parallely.
	 */
	@Override
	public StreamPipelineTaskExecutor call() {
		try(var hdfs = FileSystem.newInstance(new URI(hdfsnn), new Configuration());) {
			this.hdfs = hdfs;
			var output = new ArrayList<>();
			
			if (task.input != null && task.parentremotedatafetch != null) {
				var numinputs = task.parentremotedatafetch.length;
				for (var inputindex = 0; inputindex<numinputs;inputindex++) {
					var input = task.parentremotedatafetch[inputindex];
					if(input != null) {
						var rdf = input;
						InputStream is = RemoteDataFetcher.readIntermediatePhaseOutputFromFS(rdf.jobid, rdf.taskid);
						if (Objects.isNull(is)) {
							RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
							task.input[inputindex] = new SnappyInputStream(
									new BufferedInputStream(new ByteArrayInputStream(rdf.data)));
						} else {
							task.input[inputindex] = is;
						}
					}
				}
			}
			//Join transformation operation of map reduce stream pipelining API.
			double timetakenseconds = computeTasks(task, hdfs);
			output.clear();
		} catch (Exception ex) {
			log.error( "Stage " + task.jobid + MDCConstants.SINGLESPACE + task.stageid + " failed, See cause below \n",
					ex);
		}
		return this;
	}
	
	
}
