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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jooq.lambda.tuple.Tuple3;
import org.junit.Test;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.Context;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.ReducerValues;
import com.github.mdc.stream.StreamPipelineBaseTestCommon;

public class MassiveDataCruncherMRApiTest extends StreamPipelineBaseTestCommon {

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testMassiveDataCruncherCombiner() throws Exception {
		Combiner<String, Integer, Context> cc = (val, values, context) -> {
			int sum = 0;
			for (Integer value :values) {
				sum += value;
			}
			context.put(val, sum);
		};
		Context<String, Integer> ctx = new DataCruncherContext();
		ctx.put("PS", 100);
		ctx.put("PS", -12100);
		ctx.put("SW", -100);
		ctx.put("SW", -1200);
		CombinerExecutor mdcc = new CombinerExecutor(ctx, cc);
		Context<String, Integer> result = mdcc.call();
		assertEquals(-12000, (int) (result.get("PS").iterator().next()));
		assertEquals(-1300, (int) result.get("SW").iterator().next());
	}

	@SuppressWarnings({"rawtypes", "unchecked", "resource"})
	@Test
	public void testMassiveDataCruncherMapper() throws Exception {
		Mapper<Long, String, Context> cm = (val, line, context) -> {
			String[] contents = line.split(",");
			if (contents[0] != null && !"Year".equals(contents[0])) {
				if (contents != null && contents.length > 14 && contents[14] != null && !"NA".equals(contents[14])) {
					context.put(contents[8], Integer.parseInt(contents[14]));
				}
			}
		};
		InputStream is = MassiveDataCruncherMRApiTest.class.getResourceAsStream("/airlinesample.csv");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SnappyOutputStream lzos = new SnappyOutputStream(baos);
		lzos.write(is.readAllBytes());
		lzos.flush();
		SnappyInputStream lzis = new SnappyInputStream(new ByteArrayInputStream(baos.toByteArray()));
		MapperExecutor mdcm = new MapperExecutor(null, lzis, Arrays.asList(cm));
		Context<String, Integer> result = mdcm.call();
		assertEquals(45957l, (int) (result.get("AQ").size()));
	}


	@SuppressWarnings({"rawtypes", "unchecked", "resource"})
	@Test
	public void testMassiveDataCruncherReducer() throws Exception {
		Reducer<String, Integer, Context> cr = (val, values, context) -> {
			int sum = 0;
			for (Integer value :values) {
				sum += value;
			}
			context.put(val, sum);
		};
		DataCruncherContext<String, Integer> dcc = new DataCruncherContext();
		dcc.put("PS", 100);
		dcc.put("PS", -12100);
		dcc.put("SW", -100);
		dcc.put("SW", -1200);
		ReducerExecutor mdcc = new ReducerExecutor(dcc, cr, null);
		Context<String, Integer> result = mdcc.call();
		assertEquals(-12000, (int) (result.get("PS").iterator().next()));
		assertEquals(-1300, (int) result.get("SW").iterator().next());
	}


	@SuppressWarnings({"resource", "rawtypes", "unchecked"})
	@Test
	public void testMassiveDataTaskExecutorMapperCombiner() throws Exception {
		InputStream is = MassiveDataCruncherMRApiTest.class.getResourceAsStream("/airlinesample.csv");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SnappyOutputStream lzos = new SnappyOutputStream(baos);
		lzos.write(is.readAllBytes());
		lzos.flush();
		SnappyInputStream lzis = new SnappyInputStream(new ByteArrayInputStream(baos.toByteArray()));
		BlocksLocation bls = new BlocksLocation();
		bls.setMapperclasses(new LinkedHashSet<>(Arrays.asList(AirlineDataMapper.class.getName())));
		bls.setCombinerclasses(new LinkedHashSet<>(Arrays.asList(AirlineDataMapper.class.getName())));
		ExecutorService es = Executors.newWorkStealingPool();
		String app = MDCConstants.MDCAPPLICATION;
		String task = MDCConstants.TASK;
		TaskExecutorMapperCombiner mdtemc = new
				TaskExecutorMapperCombiner(bls, lzis, app, task, Thread.currentThread().getContextClassLoader(), 12121);		
		Context ctx = (Context) mdtemc.call();
		es.shutdown();
		assertEquals(-63278, (long) (ctx.get("AQ").iterator().next()));
	}

	@SuppressWarnings({"resource", "rawtypes", "unchecked"})
	@Test
	public void testMassiveDataTaskExecutorMapperReducer() throws Exception {
		InputStream is = MassiveDataCruncherMRApiTest.class.getResourceAsStream("/airlinesample.csv");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SnappyOutputStream lzos = new SnappyOutputStream(baos);
		lzos.write(is.readAllBytes());
		lzos.flush();
		SnappyInputStream lzis = new SnappyInputStream(new ByteArrayInputStream(baos.toByteArray()));
		BlocksLocation bls = new BlocksLocation();
		bls.setMapperclasses(new LinkedHashSet<>(Arrays.asList(AirlineDataMapper.class.getName())));
		ExecutorService es = Executors.newWorkStealingPool();
		String app = MDCConstants.MDCAPPLICATION;
		String task = MDCConstants.TASK;
		TaskExecutorMapperCombiner mdtemc = new
				TaskExecutorMapperCombiner(bls, lzis, app, task, Thread.currentThread().getContextClassLoader(), 12121);
		mdtemc.call();
		Map<String,Object> apptaskexecutormap = new ConcurrentHashMap<>();
		apptaskexecutormap.put(app + task, mdtemc);
		ReducerValues reducervalues = new ReducerValues();
		reducervalues.setTuples(Arrays.asList(new Tuple3<>("AQ", Arrays.asList(app + task), Arrays.asList("127.0.0.1_1000"))));
		reducervalues.setAppid(app);
		reducervalues.setReducerclass(AirlineDataMapper.class.getName());
		task = MDCConstants.TASK + "-1";
		TaskExecutorReducer reducerexec = new TaskExecutorReducer(reducervalues, app, task, Thread.currentThread().getContextClassLoader(), 12121, apptaskexecutormap);
		reducerexec.call();
		Context ctx = (Context) reducerexec.ctx;
		es.shutdown();
		assertEquals(-63278, (long) (ctx.get("AQ").iterator().next()));
	}
}
