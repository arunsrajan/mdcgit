package com.github.mdc.stream.executors;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.ehcache.Cache;
import org.jgroups.JChannel;

import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.RemoteDataFetch;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.Task;
import com.github.mdc.common.Utils;
import com.github.mdc.common.WhoIsResponse;

/**
 * 
 * @author Arun 
 * Task executors thread for standalone task executors daemon.
 */
@SuppressWarnings("rawtypes")
public final class StreamPipelineTaskExecutorJGroups extends StreamPipelineTaskExecutor {
	private static Logger log = Logger.getLogger(StreamPipelineTaskExecutorJGroups.class);
	private List<Task> tasks = null;
	Map<String,JobStage> jsidjsmap;
	public double timetaken = 0.0;
	public JChannel channel;
	private int port;

	public StreamPipelineTaskExecutorJGroups(Map<String,JobStage> jsidjsmap, List<Task> tasks, int port, Cache cache) {
		super(jsidjsmap.get(tasks.get(0).jobid+tasks.get(0).stageid), cache);
		this.jsidjsmap = jsidjsmap;
		this.tasks = tasks;
		this.port = port;
	}

	@Override
	public StreamPipelineTaskExecutorJGroups call() {
		log.debug("Entered MassiveDataStreamJGroupsTaskExecutor.call");
		var stagepartidstatusmap = tasks.parallelStream()
				.map(task -> task.taskid)
				.collect(Collectors.toMap(key -> key, value -> WhoIsResponse.STATUS.YETTOSTART));
		var stagepartidstatusconcmapreq = new ConcurrentHashMap<>(
				stagepartidstatusmap);
		var stagepartidstatusconcmapresp = new ConcurrentHashMap<String, WhoIsResponse.STATUS>();
		var hdfsfilepath = MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL);
		try(var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());) {
			this.hdfs = hdfs;
			channel = Utils.getChannelTaskExecutor(jobstage.jobid,
					NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST)),
					port, stagepartidstatusconcmapreq, stagepartidstatusconcmapresp);
			for (var task : tasks) {
				this.task = task;
				this.jobstage = jsidjsmap.get(task.jobid + task.stageid);
				var stageTasks = getStagesTask();
				var stagePartition = task.taskid;
				try {
					var taskspredecessor = task.taskspredecessor;
					if (!taskspredecessor.isEmpty()) {
						var stagepartitionids = taskspredecessor.parallelStream()
								.map(tk -> tk.taskid)
								.collect(Collectors.toList());
						var breakloop = false;
						while (true) {
							var stagepartidstatusconcmap = new ConcurrentHashMap<String, WhoIsResponse.STATUS>(
									stagepartidstatusconcmapreq);
							stagepartidstatusconcmap.putAll(stagepartidstatusconcmapresp);
							breakloop = true;
							for (var stagepartitionid : stagepartitionids) {
								if (stagepartidstatusconcmapresp.get(stagepartitionid) != null
										&& stagepartidstatusconcmapresp
												.get(stagepartitionid) != WhoIsResponse.STATUS.COMPLETED) {
									Utils.whois(channel, stagepartitionid);
									breakloop = false;
									continue;
								} else if (stagepartidstatusconcmap.get(stagepartitionid) != null) {
									if (stagepartidstatusconcmap
											.get(stagepartitionid) != WhoIsResponse.STATUS.COMPLETED) {
										breakloop = false;
										continue;
									}

								} else {
									Utils.whois(channel, stagepartitionid);
									breakloop = false;
									continue;
								}
							}
							if (breakloop)
								break;
							Thread.sleep(1000);
						}
					}
					log.debug("Submitted Stage " + stagePartition);					
					log.debug("Running Stage " + stageTasks);
					
					stagepartidstatusconcmapreq.put(stagePartition, WhoIsResponse.STATUS.RUNNING);
					if (task.input != null && task.parentremotedatafetch != null) {
						var numinputs = task.parentremotedatafetch.length;
						for (var inputindex = 0; inputindex < numinputs; inputindex++) {
							var input = task.parentremotedatafetch[inputindex];
							if (input != null) {
								var rdf = (RemoteDataFetch) input;
								task.input[inputindex] = RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(
										rdf.jobid, getIntermediateDataFSFilePath(rdf.jobid, rdf.stageid, rdf.taskid),
										hdfs);
							}
						}
					}

					var timetakenseconds = computeTasks(task, hdfs);
					log.debug("Completed Stage " + stagePartition+" in "+timetakenseconds);
					stagepartidstatusconcmapreq.put(stagePartition, WhoIsResponse.STATUS.COMPLETED);
				} catch (Exception ex) {

				}
			}
			log.debug("StagePartitionId with Stage Statuses: " + stagepartidstatusconcmapreq
					+ " WhoIs Response stauses: " + stagepartidstatusconcmapresp);
		} catch (Exception ex) {
			log.debug("Failed Stage " + tasks, ex);
		}
		log.debug("Exiting MassiveDataStreamJGroupsTaskExecutor.call");
		return this;
	}

}
