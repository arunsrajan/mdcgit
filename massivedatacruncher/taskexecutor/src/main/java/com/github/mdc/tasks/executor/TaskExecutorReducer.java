package com.github.mdc.tasks.executor;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple2;

import com.github.mdc.common.Context;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.HeartBeatTaskScheduler;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCExecutorThreadFactory;
import com.github.mdc.common.ReducerValues;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.ApplicationTask.TaskStatus;
import com.github.mdc.common.ApplicationTask.TaskType;

public class TaskExecutorReducer implements Runnable{
	static Logger log = Logger.getLogger(TaskExecutorReducer.class);
	@SuppressWarnings("rawtypes")
	Reducer cr = null;
	ReducerValues rv;
	File file;
	@SuppressWarnings("rawtypes")
	Context ctx;
	HeartBeatTaskScheduler hbts;
	String applicationid;
	String taskid;
	ExecutorService taskpool;
	int port;
	@SuppressWarnings({ "rawtypes" })
	public TaskExecutorReducer(ReducerValues rv,String applicationid, String taskid,
			ExecutorService taskpool,ClassLoader cl,int port,
			HeartBeatTaskScheduler hbts) throws Exception {
		this.rv = rv;
		Class<?> clz = null;
		this.port = port;
		try {
			clz = cl.loadClass(rv.reducerclass);
			cr = (Reducer) clz.newInstance();
			this.applicationid = applicationid;
			this.taskid = taskid;
			this.taskpool = taskpool;
		}
		catch(Exception ex) {
			log.debug("Exception in loading class:",ex);
		}
		finally {
			
		}
		this.hbts = hbts;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void run() {
		var es = Executors.newWorkStealingPool();
		try {
			hbts.pingOnce(taskid, TaskStatus.RUNNING, TaskType.REDUCER, null);
			log.debug("Submitted Reducer:"+applicationid+taskid);
			var complete = new DataCruncherContext();
			var apptaskcontextmap = new ConcurrentHashMap<String,Context>();
			Context currentctx;
			for (var tuple2 : (List<Tuple2>)rv.tuples) {
				var ctx = new DataCruncherContext();
				for (var apptaskids : (Collection<String>) tuple2.v2) {
					if(apptaskcontextmap.get(apptaskids)!=null) {
						currentctx = apptaskcontextmap.get(apptaskids);
					}
					else {
						currentctx = (Context) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(rv.appid,
								(apptaskids + MDCConstants.DATAFILEEXTN), false);
						apptaskcontextmap.put(apptaskids, currentctx);
					}
					ctx.addAll(tuple2.v1, currentctx.get(tuple2.v1));
				}
				var mdcr = new ReducerExecutor((DataCruncherContext) ctx, cr,
						tuple2.v1);
				var fc = es.submit(mdcr);
				var results = fc.get();
				complete.add(results);
			}
			RemoteDataFetcher.writerIntermediatePhaseOutputToDFS(complete, applicationid,
					((applicationid + taskid) + MDCConstants.DATAFILEEXTN));
			ctx = null;
			hbts.pingOnce(taskid, TaskStatus.COMPLETED, TaskType.REDUCER, null);
			log.debug("Submitted Reducer Completed:"+applicationid+taskid);
		} catch (Throwable ex) {
			try {
				var baos = new ByteArrayOutputStream();
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
				hbts.pingOnce(taskid, TaskStatus.FAILED, TaskType.REDUCER, new String(baos.toByteArray()));
			} catch (Exception e) {
				log.error("Send Message Error For Task Failed: ",e);
			}
			log.error("Submitted Reducer Failed:",ex);
		} finally {
			if(es!=null) {
				es.shutdown();
			}
			if(taskpool!=null) {
				taskpool.shutdown();
			}
		}
	}

	public HeartBeatTaskScheduler getHbts() {
		return hbts;
	}
	
	
	
}
