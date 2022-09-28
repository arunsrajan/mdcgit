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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple3;

import com.github.mdc.common.Context;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.ReducerValues;
import com.github.mdc.common.RetrieveData;
import com.github.mdc.common.Utils;

public class TaskExecutorReducer implements Runnable {
	static Logger log = Logger.getLogger(TaskExecutorReducer.class);
	@SuppressWarnings("rawtypes")
	Reducer cr;
	ReducerValues rv;
	File file;
	@SuppressWarnings("rawtypes")
	Context ctx;
	String applicationid;
	String taskid;
	int port;
	Map<String, Object> apptaskexecutormap;

	@SuppressWarnings({"rawtypes"})
	public TaskExecutorReducer(ReducerValues rv, String applicationid, String taskid,
			ClassLoader cl, int port,
			Map<String, Object> apptaskexecutormap) throws Exception {
		this.rv = rv;
		Class<?> clz = null;
		this.port = port;
		try {
			clz = cl.loadClass(rv.getReducerclass());
			cr = (Reducer) clz.getDeclaredConstructor().newInstance();
			this.applicationid = applicationid;
			this.taskid = taskid;
		}
		catch (Exception ex) {
			log.debug("Exception in loading class:", ex);
		}
		this.apptaskexecutormap = apptaskexecutormap;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Override
	public void run() {
		var es = Executors.newSingleThreadExecutor();
		try {
			log.debug("Submitted Reducer:" + applicationid + taskid);
			var complete = new DataCruncherContext();
			var apptaskcontextmap = new ConcurrentHashMap<String, Context>();
			Context currentctx;
			for (var tuple3 : (List<Tuple3>) rv.getTuples()) {
				var ctx = new DataCruncherContext();
				int hpcount = 0;
				for (var apptaskids : (Collection<String>) tuple3.v2) {
					if (apptaskcontextmap.get(apptaskids) != null) {
						currentctx = apptaskcontextmap.get(apptaskids);
					}
					else {
						TaskExecutorMapperCombiner temc = (TaskExecutorMapperCombiner) apptaskexecutormap.get(apptaskids);
						currentctx = (Context) temc.ctx;
						if(currentctx == null) {
							var objects = new ArrayList<>();
							objects.add(new RetrieveData());
							objects.add(applicationid);
							objects.add(apptaskids.replace(applicationid, MDCConstants.EMPTY));
							currentctx = (Context) Utils.getResultObjectByInput((String)((List)tuple3.v3).get(hpcount), objects);
						}
						apptaskcontextmap.put(apptaskids, currentctx);
					}
					ctx.addAll(tuple3.v1, currentctx.get(tuple3.v1));
					hpcount++;
				}
				var mdcr = new ReducerExecutor((DataCruncherContext) ctx, cr,
						tuple3.v1);
				var fc = es.submit(mdcr);
				var results = fc.get();
				complete.add(results);
			}
			ctx = complete;
			log.debug("Submitted Reducer Completed:" + applicationid + taskid);
		} catch (Throwable ex) {
			try {
				var baos = new ByteArrayOutputStream();
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
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

}
