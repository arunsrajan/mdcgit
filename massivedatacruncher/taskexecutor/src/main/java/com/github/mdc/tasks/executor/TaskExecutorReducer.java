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
package com.github.mdc.tasks.executor;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple2;

import com.github.mdc.common.Context;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.HeartBeatTaskScheduler;
import com.github.mdc.common.ReducerValues;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.ApplicationTask.TaskStatus;
import com.github.mdc.common.ApplicationTask.TaskType;

public class TaskExecutorReducer implements Runnable {
	static Logger log = Logger.getLogger(TaskExecutorReducer.class);
	@SuppressWarnings("rawtypes")
	Reducer cr;
	ReducerValues rv;
	File file;
	@SuppressWarnings("rawtypes")
	Context ctx;
	HeartBeatTaskScheduler hbts;
	String applicationid;
	String taskid;
	int port;

	@SuppressWarnings({"rawtypes"})
	public TaskExecutorReducer(ReducerValues rv, String applicationid, String taskid,
			ClassLoader cl, int port,
			HeartBeatTaskScheduler hbts) throws Exception {
		this.rv = rv;
		Class<?> clz = null;
		this.port = port;
		try {
			clz = cl.loadClass(rv.reducerclass);
			cr = (Reducer) clz.getDeclaredConstructor().newInstance();
			this.applicationid = applicationid;
			this.taskid = taskid;
		}
		catch (Exception ex) {
			log.debug("Exception in loading class:", ex);
		}
		this.hbts = hbts;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Override
	public void run() {
		var es = Executors.newSingleThreadExecutor();
		try {
			hbts.pingOnce(taskid, TaskStatus.RUNNING, TaskType.REDUCER, null);
			log.debug("Submitted Reducer:" + applicationid + taskid);
			var complete = new DataCruncherContext();
			var apptaskcontextmap = new ConcurrentHashMap<String, Context>();
			Context currentctx;
			for (var tuple2 : (List<Tuple2>) rv.tuples) {
				var ctx = new DataCruncherContext();
				for (var apptaskids : (Collection<String>) tuple2.v2) {
					if (apptaskcontextmap.get(apptaskids) != null) {
						currentctx = apptaskcontextmap.get(apptaskids);
					}
					else {
						currentctx = (Context) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(rv.appid,
								apptaskids, false);
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
					(applicationid + taskid));
			ctx = null;
			hbts.pingOnce(taskid, TaskStatus.COMPLETED, TaskType.REDUCER, null);
			log.debug("Submitted Reducer Completed:" + applicationid + taskid);
		} catch (Throwable ex) {
			try {
				var baos = new ByteArrayOutputStream();
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
				hbts.pingOnce(taskid, TaskStatus.FAILED, TaskType.REDUCER, new String(baos.toByteArray()));
			} catch (Exception e) {
				log.error("Send Message Error For Task Failed: ", e);
			}
			log.error("Submitted Reducer Failed:", ex);
		} finally {
			if (es != null) {
				es.shutdown();
			}
		}
	}

	public HeartBeatTaskScheduler getHbts() {
		return hbts;
	}


}
