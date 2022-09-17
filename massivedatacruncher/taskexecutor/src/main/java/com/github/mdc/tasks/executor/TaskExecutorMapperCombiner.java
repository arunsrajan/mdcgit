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
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.github.mdc.common.ApplicationTask.TaskStatus;
import com.github.mdc.common.ApplicationTask.TaskType;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.Context;
import com.github.mdc.common.HeartBeatTaskScheduler;

public class TaskExecutorMapperCombiner implements Runnable {
	static Logger log = Logger.getLogger(TaskExecutorMapperCombiner.class);
	BlocksLocation blockslocation;
	@SuppressWarnings("rawtypes")
	List<Mapper> cm = new ArrayList<>();
	@SuppressWarnings("rawtypes")
	List<Combiner> cc = new ArrayList<>();
	@SuppressWarnings("rawtypes")
	Context ctx;
	File file;
	HeartBeatTaskScheduler hbts;
	String applicationid;
	String taskid;
	InputStream datastream;
	int port;

	@SuppressWarnings({"rawtypes"})
	public TaskExecutorMapperCombiner(BlocksLocation blockslocation, InputStream datastream, String applicationid, String taskid,
									  ClassLoader cl, int port,
									  HeartBeatTaskScheduler hbts) throws Exception {
		this.blockslocation = blockslocation;
		this.datastream = datastream;
		this.port = port;
		Class<?> clz = null;
		try {
			if (blockslocation.getMapperclasses() != null) {
				for (var mapperclass :blockslocation.getMapperclasses()) {
					clz = cl.loadClass(mapperclass);
					cm.add((Mapper) clz.getDeclaredConstructor().newInstance());
				}
			}
			if (blockslocation.getCombinerclasses() != null) {
				for (var combinerclass :blockslocation.getCombinerclasses()) {
					clz = cl.loadClass(combinerclass);
					cc.add((Combiner) clz.getDeclaredConstructor().newInstance());
				}
			}
		}
		catch (Throwable ex) {
			log.debug("Exception in loading class:", ex);
		}
		this.applicationid = applicationid;
		this.taskid = taskid;
		this.hbts = hbts;
	}

	@Override
	public void run() {
		var es = Executors.newSingleThreadExecutor();

		try {
			hbts.pingOnce(taskid, TaskStatus.SUBMITTED, TaskType.MAPPERCOMBINER, null);

			var mdcmc = new MapperCombinerExecutor(blockslocation, datastream, cm, cc);
			hbts.pingOnce(taskid, TaskStatus.RUNNING, TaskType.MAPPERCOMBINER, null);
			var fc = es.submit(mdcmc);
			ctx = fc.get();
			
			hbts.pingOnce(taskid, TaskStatus.COMPLETED, TaskType.MAPPERCOMBINER, null);
		} catch (Throwable ex) {
			try {
				var baos = new ByteArrayOutputStream();
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
				hbts.pingOnce(taskid, TaskStatus.FAILED, TaskType.MAPPERCOMBINER, new String(baos.toByteArray()));
			} catch (Exception e) {
				log.info("Exception in Sending message to Failed Task: " + blockslocation, ex);
			}
			log.info("Exception in Executing Task: " + blockslocation, ex);
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
