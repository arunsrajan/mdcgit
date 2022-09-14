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
package com.github.mdc.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.DAGEdge;
import com.github.mdc.common.HeartBeatStream;
import com.github.mdc.common.HeartBeatTaskObserver;
import com.github.mdc.common.HeartBeatTaskSchedulerStream;
import com.github.mdc.common.Job;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCConstants.STORAGE;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Stage;
import com.github.mdc.common.Task;
import com.github.mdc.stream.scheduler.StreamJobScheduler;
import com.github.mdc.stream.scheduler.StreamPipelineTaskSubmitter;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StreamJobSchedulerTest extends StreamPipelineBaseTestCommon {

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testGeneratePhysicalExecutionPlanIntersection() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);

		StreamPipeline<String> mdp1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);

		StreamPipeline<String> intersection = mdp.filter(val -> true).intersection(mdp1.filter(val -> true));

		((StreamPipeline) intersection.root).finaltasks.add(intersection.task);
		((StreamPipeline) intersection.root).mdsroots.add(mdp);
		Job job = ((StreamPipeline) intersection.root).createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(3, graph.vertexSet().size());
		assertEquals(2, graph.edgeSet().size());
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testGeneratePhysicalExecutionPlanIntersectionPartitioned() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String> mdp1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String> intersection = mdp.filter(val -> true).intersection(mdp1.filter(val -> true));

		((StreamPipeline) intersection.root).finaltasks.add(intersection.task);
		((StreamPipeline) intersection.root).mdsroots.add(mdp);
		Job job = ((StreamPipeline) intersection.root).createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(35, graph.vertexSet().size());
		assertEquals(50, graph.edgeSet().size());
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testGeneratePhysicalExecutionPlanJoin() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter = mdparr
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair = filter.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);

		StreamPipeline<String> mdp1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr1 = mdp1.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter1 = mdparr1
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair1 = filter1.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey1 = mappair1.reduceByKey((a, b) -> a + b);
		MapPair<Tuple2, Tuple2> join = reducebykey.join(reducebykey1, (tup1, tup2) -> tup1.v1.equals(tup2.v1));

		((StreamPipeline) join.root).finaltasks.add(join.task);
		((StreamPipeline) join.root).mdsroots.add(mdp);
		Job job = ((StreamPipeline) join.root).createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(3, graph.vertexSet().size());
		assertEquals(2, graph.edgeSet().size());
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testGeneratePhysicalExecutionPlanJoinPartitioned() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter = mdparr
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair = filter.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);

		StreamPipeline<String> mdp1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr1 = mdp1.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter1 = mdparr1
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair1 = filter1.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey1 = mappair1.reduceByKey((a, b) -> a + b);
		MapPair<Tuple2, Tuple2> join = reducebykey.join(reducebykey1, (tup1, tup2) -> tup1.v1.equals(tup2.v1));

		((StreamPipeline) join.root).finaltasks.add(join.task);
		((StreamPipeline) join.root).mdsroots.add(mdp);
		Job job = ((StreamPipeline) join.root).createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(35, graph.vertexSet().size());
		assertEquals(50, graph.edgeSet().size());
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testGeneratePhysicalExecutionPlanLeftJoin() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter = mdparr
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair = filter.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);

		StreamPipeline<String> mdp1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr1 = mdp1.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter1 = mdparr1
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair1 = filter1.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey1 = mappair1.reduceByKey((a, b) -> a + b);
		MapPair<String, Long> join = reducebykey.leftOuterjoin(reducebykey1, (tup1, tup2) -> tup1.v1.equals(tup2.v1));

		((StreamPipeline) join.root).finaltasks.add(join.task);
		((StreamPipeline) join.root).mdsroots.add(mdp);
		Job job = ((StreamPipeline) join.root).createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(3, graph.vertexSet().size());
		assertEquals(2, graph.edgeSet().size());
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testGeneratePhysicalExecutionPlanLeftJoinPartitioned() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter = mdparr
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair = filter.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);

		StreamPipeline<String> mdp1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr1 = mdp1.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter1 = mdparr1
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair1 = filter1.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey1 = mappair1.reduceByKey((a, b) -> a + b);
		MapPair<String, Long> join = reducebykey.leftOuterjoin(reducebykey1, (tup1, tup2) -> tup1.v1.equals(tup2.v1));

		((StreamPipeline) join.root).finaltasks.add(join.task);
		((StreamPipeline) join.root).mdsroots.add(mdp);

		Job job = ((StreamPipeline) join.root).createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(35, graph.vertexSet().size());
		assertEquals(50, graph.edgeSet().size());
	}

	@Test
	public void testGeneratePhysicalExecutionPlanMap() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(1, graph.vertexSet().size());
		assertEquals(0, graph.edgeSet().size());
	}

	@Test
	public void testGeneratePhysicalExecutionPlanMapFilterMapPairReduceByKey() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter = mdparr
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair = filter.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);
		mdparr.finaltasks.add(reducebykey.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(1, graph.vertexSet().size());
		assertEquals(0, graph.edgeSet().size());
	}

	@Test
	public void testGeneratePhysicalExecutionPlanMapFilterMapPairReduceByKeyCoalesce() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter = mdparr
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair = filter.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);
		MapPair<String, Long> coalesce = reducebykey.coalesce(1, (a, b) -> a + b);
		mdparr.finaltasks.add(coalesce.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(2, graph.vertexSet().size());
		assertEquals(1, graph.edgeSet().size());
	}

	@Test
	public void testGeneratePhysicalExecutionPlanMapFilterMapPairReduceByKeyCoalescePartitioned() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter = mdparr
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair = filter.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);
		MapPair<String, Long> coalesce = reducebykey.coalesce(1, (a, b) -> a + b);
		mdparr.finaltasks.add(coalesce.task);
		mdparr.finaltasks.add(reducebykey.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(6, graph.vertexSet().size());
		assertEquals(5, graph.edgeSet().size());
	}

	@Test
	public void testGeneratePhysicalExecutionPlanMapFilterMapPairReduceByKeyPartitioned() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter = mdparr
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair = filter.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);
		mdparr.finaltasks.add(reducebykey.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(5, graph.vertexSet().size());
		assertEquals(0, graph.edgeSet().size());
	}

	@Test
	public void testGeneratePhysicalExecutionPlanMapMultiplePartition() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(5, graph.vertexSet().size());
		assertEquals(0, graph.edgeSet().size());
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testGeneratePhysicalExecutionPlanRightJoin() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter = mdparr
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair = filter.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);

		StreamPipeline<String> mdp1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr1 = mdp1.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter1 = mdparr1
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair1 = filter1.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey1 = mappair1.reduceByKey((a, b) -> a + b);
		MapPair<String, Long> join = reducebykey.rightOuterjoin(reducebykey1, (tup1, tup2) -> tup1.v1.equals(tup2.v1));

		((StreamPipeline) join.root).finaltasks.add(join.task);
		((StreamPipeline) join.root).mdsroots.add(mdp);

		Job job = ((StreamPipeline) join.root).createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(3, graph.vertexSet().size());
		assertEquals(2, graph.edgeSet().size());
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testGeneratePhysicalExecutionPlanRightJoinPartitioned() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter = mdparr
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair = filter.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);

		StreamPipeline<String> mdp1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr1 = mdp1.map(val -> val.split(MDCConstants.COMMA));
		StreamPipeline<String[]> filter1 = mdparr1
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> mappair1 = filter1.mapToPair(val -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey1 = mappair1.reduceByKey((a, b) -> a + b);
		MapPair<String, Long> join = reducebykey.rightOuterjoin(reducebykey1, (tup1, tup2) -> tup1.v1.equals(tup2.v1));

		((StreamPipeline) join.root).finaltasks.add(join.task);
		((StreamPipeline) join.root).mdsroots.add(mdp);

		Job job = ((StreamPipeline) join.root).createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(35, graph.vertexSet().size());
		assertEquals(50, graph.edgeSet().size());
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testGeneratePhysicalExecutionPlanUnion() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);

		StreamPipeline<String> mdp1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);

		StreamPipeline<String> union = mdp.filter(val -> true).union(mdp1.filter(val -> true));

		((StreamPipeline) union.root).finaltasks.add(union.task);
		((StreamPipeline) union.root).mdsroots.add(mdp);
		((StreamPipeline) union.root).mdsroots.add(mdp1);

		Job job = ((StreamPipeline) union.root).createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(3, graph.vertexSet().size());
		assertEquals(2, graph.edgeSet().size());
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testGeneratePhysicalExecutionPlanUnionPartitioned() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String> mdp1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String> union = mdp.filter(val -> true).union(mdp1.filter(val -> true));

		((StreamPipeline) union.root).finaltasks.add(union.task);
		((StreamPipeline) union.root).mdsroots.add(mdp);
		Job job = ((StreamPipeline) union.root).createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		assertEquals(35, graph.vertexSet().size());
		assertEquals(50, graph.edgeSet().size());
	}

	@SuppressWarnings("resource")
	@Test
	public void testGetContainersHostPort() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("false");
		pc.setJgroups("false");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		js.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		HeartBeatStream hbss = new HeartBeatStream();
		js.hbss = hbss;
		js.getContainersHostPort();
		assertEquals(job.getContainers().size(), js.taskexecutors.size());
		assertTrue(job.getContainers().containsAll(js.taskexecutors));
		js.destroyContainers();
		pc.setLocal("true");
	}

	@SuppressWarnings({"rawtypes", "unchecked", "resource"})
	@Test
	public void testParallelExecutionPhaseDExecutor() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("false");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		pc.setGctype(MDCConstants.ZGC);
		pc.setStorage(STORAGE.INMEMORY);
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		job.setTrigger(Job.TRIGGER.COLLECT);
		StreamJobScheduler js = new StreamJobScheduler();
		js.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		js.batchsize = Integer.parseInt(pc.getBatchsize());
		js.hbtss = new HeartBeatTaskSchedulerStream();
		js.hbtss.init(Integer.parseInt(pc.getRescheduledelay()),
				Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT)),
				MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST),
				Integer.parseInt(pc.getInitialdelay()), Integer.parseInt(pc.getPingdelay()), MDCConstants.EMPTY,
				 job.getId());
		// Start the heart beat to receive task executor to task
		// schedulers task status updates.
		js.hbtss.start();		
		HeartBeatStream hbss = new HeartBeatStream();
		js.hbss = hbss;
		js.getContainersHostPort();
		js.hbss = hbss;
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			JobStage jobstage = new JobStage();
			jobstage.jobid = job.getId();
			jobstage.stageid = stage.id;
			jobstage.stage = stage;
			js.jsidjsmap.put( job.getId() + stage.id, jobstage);
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		js.broadcastJobStageToTaskExecutors();
		Iterator<StreamPipelineTaskSubmitter> topostages = new TopologicalOrderIterator(graph);
		List<StreamPipelineTaskSubmitter> mdststs = new ArrayList<>();
		// Sequential ordering of topological ordering is obtained to
		// process for parallelization.
		while (topostages.hasNext()) {
			mdststs.add(topostages.next());
		}
		var mdstts = js.getFinalPhasesWithNoSuccessors(graph, mdststs);
		var partitionnumber = 0;
		for (var mdstst :mdstts) {
			mdstst.getTask().finalphase = true;
			mdstst.getTask().hdfsurl = job.getUri();
			mdstst.getTask().filepath = job.getSavepath() + MDCConstants.HYPHEN + partitionnumber++;
			mdstst.getTask().hbphysicaladdress = js.hbtss.getPhysicalAddress();
		}
		job.getContainers().parallelStream().forEach(container->js.hbtss.getHbo().put(container, new HeartBeatTaskObserver<>()));
		js.hbtss.getHbo().values().forEach(hbo-> {
			try {
				hbo.start();
			} catch (Exception e) {
				log.error("HeartBeat Observer start error");
			}
		});
		js.parallelExecutionPhaseDExecutor(graph);
		List<List> results = js.getLastStageOutput(mdstts, graph, mdststs, false, false, false, false, js.resultstream);
		int sum = 0;
		for (List result : results) {
			sum += result.size();
		}
		assertEquals(46361, sum);
		js.hbtss.getHbo().values().forEach(hbo-> {
			try {
				hbo.stop();
			} catch (Exception e) {
				log.error("HeartBeat Observer start error");
			}
		});
		js.hbtss.close();
		js.hbss.stop();
		js.hbss.destroy();
		js.destroyContainers();
		pc.setLocal("true");
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testParallelExecutionPhaseDExecutorLocalMode() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setBatchsize("1");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		js.batchsize = Integer.parseInt(pc.getBatchsize());
		js.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			JobStage jobstage = new JobStage();
			jobstage.jobid = job.getId();
			jobstage.stageid = stage.id;
			jobstage.stage = stage;
			js.jsidjsmap.put( job.getId() + stage.id, jobstage);
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		Iterator<StreamPipelineTaskSubmitter> topostages = new TopologicalOrderIterator(graph);
		List<StreamPipelineTaskSubmitter> mdststs = new ArrayList<>();
		// Sequential ordering of topological ordering is obtained to
		// process for parallelization.
		while (topostages.hasNext()) {
			mdststs.add(topostages.next());
		}
		var mdstts = js.getFinalPhasesWithNoSuccessors(graph, mdststs);
		var partitionnumber = 0;
		for (var mdstst :mdstts) {
			mdstst.getTask().finalphase = true;
			mdstst.getTask().hdfsurl = job.getUri();
			mdstst.getTask().filepath = job.getSavepath() + MDCConstants.HYPHEN + partitionnumber++;
		}
		js.parallelExecutionPhaseDExecutorLocalMode(graph, js.new TaskProviderLocalMode(graph.vertexSet().size()));
		List<List> result = js.getLastStageOutput(mdstts, graph, mdststs, false, false, true, false, js.resultstream);
		assertEquals(46361, result.get(0).size());
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testParallelExecutionPhaseDExecutorLocalModeMultiplePartition() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		js.batchsize = Integer.parseInt(pc.getBatchsize());
		js.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			JobStage jobstage = new JobStage();
			jobstage.jobid = job.getId();
			jobstage.stageid = stage.id;
			jobstage.stage = stage;
			js.jsidjsmap.put( job.getId() + stage.id, jobstage);
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		Iterator<StreamPipelineTaskSubmitter> topostages = new TopologicalOrderIterator(graph);
		List<StreamPipelineTaskSubmitter> mdststs = new ArrayList<>();
		// Sequential ordering of topological ordering is obtained to
		// process for parallelization.
		while (topostages.hasNext()) {
			mdststs.add(topostages.next());
		}
		var mdstts = js.getFinalPhasesWithNoSuccessors(graph, mdststs);
		var partitionnumber = 0;
		for (var mdstst :mdstts) {
			mdstst.getTask().finalphase = true;
			mdstst.getTask().hdfsurl = job.getUri();
			mdstst.getTask().filepath = job.getSavepath() + MDCConstants.HYPHEN + partitionnumber++;
		}
		js.parallelExecutionPhaseDExecutorLocalMode(graph, js.new TaskProviderLocalMode(graph.vertexSet().size()));
		List<List> results = js.getLastStageOutput(mdstts, graph, mdststs, false, false, true, false, js.resultstream);
		int sum = 0;
		for (List result : results) {
			sum += result.size();
		}
		assertEquals(46361, sum);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testParallelExecutionPhaseDExecutorLocalModeMultiplePartitionBatchSize() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		pc.setBatchsize("3");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		js.batchsize = Integer.parseInt(pc.getBatchsize());
		js.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.getTopostages());
		int stagenumber = 0;
		SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			JobStage jobstage = new JobStage();
			jobstage.jobid = job.getId();
			jobstage.stageid = stage.id;
			jobstage.stage = stage;
			js.jsidjsmap.put( job.getId() + stage.id, jobstage);
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph, taskgraph);
			stagenumber++;
		}
		Iterator<StreamPipelineTaskSubmitter> topostages = new TopologicalOrderIterator(graph);
		List<StreamPipelineTaskSubmitter> mdststs = new ArrayList<>();
		// Sequential ordering of topological ordering is obtained to
		// process for parallelization.
		while (topostages.hasNext()) {
			mdststs.add(topostages.next());
		}
		var mdstts = js.getFinalPhasesWithNoSuccessors(graph, mdststs);
		var partitionnumber = 0;
		for (var mdstst :mdstts) {
			mdstst.getTask().finalphase = true;
			mdstst.getTask().hdfsurl = job.getUri();
			mdstst.getTask().filepath = job.getSavepath() + MDCConstants.HYPHEN + partitionnumber++;
		}
		js.parallelExecutionPhaseDExecutorLocalMode(graph, js.new TaskProviderLocalMode(graph.vertexSet().size()));
		List<List> results = js.getLastStageOutput(mdstts, graph, mdststs, false, false, true, false, js.resultstream);
		int sum = 0;
		for (List result : results) {
			sum += result.size();
		}
		assertEquals(46361, sum);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testScheduleJobJGroups() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("false");
		pc.setJgroups("true");
		pc.setStorage(MDCConstants.STORAGE.DISK);
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);
		Job job = mdparr.createJob();
		job.setTrigger(Job.TRIGGER.COLLECT);
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		List<List> result = (List<List>) js.schedule(job);
		assertEquals(1, result.size());
		assertEquals(46361, result.get(0).size());
		pc.setJgroups("false");
		pc.setLocal("true");
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testScheduleJobLocal() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		List<List> result = (List<List>) js.schedule(job);
		assertEquals(1, result.size());
		assertEquals(46361, result.get(0).size());
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testScheduleJobStandalone1() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("false");
		pc.setJgroups("false");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);
		Job job = mdparr.createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		List<List> result = (List<List>) js.schedule(job);
		assertEquals(1, result.size());
		assertEquals(46361, result.get(0).size());
		pc.setLocal("true");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testScheduleJobStandalone2() throws Exception {
		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("false");
		pc.setJgroups("false");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		StreamPipeline<String[]> mdparr = mdp.map(val -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);
		Job job = mdparr.createJob();
		StreamJobScheduler js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		List<List> result = (List<List>) js.schedule(job);
		assertEquals(1, result.size());
		assertEquals(46361, result.get(0).size());
		pc.setLocal("true");
	}

}
