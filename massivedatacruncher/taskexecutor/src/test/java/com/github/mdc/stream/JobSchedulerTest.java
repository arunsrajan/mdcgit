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
import org.junit.Test;

import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.DAGEdge;
import com.github.mdc.common.HeartBeatServerStream;
import com.github.mdc.common.HeartBeatTaskSchedulerStream;
import com.github.mdc.common.Job;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Stage;
import com.github.mdc.common.Task;
import com.github.mdc.stream.MapPair;
import com.github.mdc.stream.MassiveDataPipeline;
import com.github.mdc.stream.scheduler.JobScheduler;
import com.github.mdc.stream.scheduler.MassiveDataStreamTaskSchedulerThread;

public class JobSchedulerTest extends MassiveDataPipelineBaseTestClassCommon {
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testScheduleJobLocal() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		List<List> result = (List<List>) js.schedule(job);
		assertEquals(1, result.size());
		assertEquals(46361, result.get(0).size());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testScheduleJobJGroups() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("false");
		pc.setJgroups("true");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);
		Job job = mdparr.createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		List<List> result = (List<List>) js.schedule(job);
		assertEquals(1, result.size());
		assertEquals(46361, result.get(0).size());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testScheduleJobStandalone1() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("false");
		pc.setJgroups("false");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);
		Job job = mdparr.createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		List<List> result = (List<List>) js.schedule(job);
		assertEquals(1, result.size());
		assertEquals(46361, result.get(0).size());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testScheduleJobStandalone2() throws Exception {
		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("false");
		pc.setJgroups("false");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);
		Job job = mdparr.createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		List<List> result = (List<List>) js.schedule(job);
		assertEquals(1, result.size());
		assertEquals(46361, result.get(0).size());
	}

	@SuppressWarnings("resource")
	@Test
	public void testGetContainersHostPort() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("false");
		pc.setJgroups("false");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		JobScheduler js = new JobScheduler();
		js.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		HeartBeatServerStream hbss = new HeartBeatServerStream();
		js.hbss = hbss;
		js.getContainersHostPort();
		assertEquals(job.containers.size(), js.taskexecutors.size());
		assertTrue(job.containers.containsAll(js.taskexecutors));
		js.destroyContainers();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testParallelExecutionPhaseDExecutorLocalMode() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setBatchsize("1");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		JobScheduler js = new JobScheduler();
		js.batchsize = Integer.parseInt(pc.getBatchsize()); 
		js.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			JobStage jobstage = new JobStage();
			jobstage.jobid = job.id;
			jobstage.stageid = stage.id;
			jobstage.stage = stage;
			js.jsidjsmap.put(job.id + stage.id, jobstage);
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		Iterator<MassiveDataStreamTaskSchedulerThread> topostages = new TopologicalOrderIterator(graph);
		List<MassiveDataStreamTaskSchedulerThread> mdststs = new ArrayList<>();
		// Sequential ordering of topological ordering is obtained to
		// process for parallelization.
		while (topostages.hasNext())
			mdststs.add(topostages.next());
		js.parallelExecutionPhaseDExecutorLocalMode(graph, js.new TaskProviderLocalMode(graph.vertexSet().size()));
		List<List> result = js.getLastStageOutput(graph, mdststs, false, false, true, false, js.resultstream);
		assertEquals(46361, result.get(0).size());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testParallelExecutionPhaseDExecutorLocalModeMultiplePartition() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		JobScheduler js = new JobScheduler();
		js.batchsize = Integer.parseInt(pc.getBatchsize()); 
		js.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			JobStage jobstage = new JobStage();
			jobstage.jobid = job.id;
			jobstage.stageid = stage.id;
			jobstage.stage = stage;
			js.jsidjsmap.put(job.id + stage.id, jobstage);
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		Iterator<MassiveDataStreamTaskSchedulerThread> topostages = new TopologicalOrderIterator(graph);
		List<MassiveDataStreamTaskSchedulerThread> mdststs = new ArrayList<>();
		// Sequential ordering of topological ordering is obtained to
		// process for parallelization.
		while (topostages.hasNext())
			mdststs.add(topostages.next());
		js.parallelExecutionPhaseDExecutorLocalMode(graph, js.new TaskProviderLocalMode(graph.vertexSet().size()));
		List<List> results = js.getLastStageOutput(graph, mdststs, false, false, true, false, js.resultstream);
		int sum = 0;
		for (List result : results) {
			sum += result.size();
		}
		assertEquals(46361, sum);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testParallelExecutionPhaseDExecutorLocalModeMultiplePartitionBatchSize() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		pc.setBatchsize("3");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		JobScheduler js = new JobScheduler();
		js.batchsize = Integer.parseInt(pc.getBatchsize());
		js.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			JobStage jobstage = new JobStage();
			jobstage.jobid = job.id;
			jobstage.stageid = stage.id;
			jobstage.stage = stage;
			js.jsidjsmap.put(job.id + stage.id, jobstage);
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		Iterator<MassiveDataStreamTaskSchedulerThread> topostages = new TopologicalOrderIterator(graph);
		List<MassiveDataStreamTaskSchedulerThread> mdststs = new ArrayList<>();
		// Sequential ordering of topological ordering is obtained to
		// process for parallelization.
		while (topostages.hasNext())
			mdststs.add(topostages.next());
		js.parallelExecutionPhaseDExecutorLocalMode(graph, js.new TaskProviderLocalMode(graph.vertexSet().size()));
		List<List> results = js.getLastStageOutput(graph, mdststs, false, false, true, false, js.resultstream);
		int sum = 0;
		for (List result : results) {
			sum += result.size();
		}
		assertEquals(46361, sum);
	}

	@Test
	public void testGeneratePhysicalExecutionPlanMap() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		assertEquals(1, graph.vertexSet().size());
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
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		assertEquals(5, graph.vertexSet().size());
		assertEquals(0, graph.edgeSet().size());
	}

	@Test
	public void testGeneratePhysicalExecutionPlanMapFilterMapPairReduceByKey() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter = mdparr
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair = filter.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);
		mdparr.finaltasks.add(reducebykey.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		assertEquals(1, graph.vertexSet().size());
		assertEquals(0, graph.edgeSet().size());
	}

	@Test
	public void testGeneratePhysicalExecutionPlanMapFilterMapPairReduceByKeyPartitioned() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter = mdparr
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair = filter.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);
		mdparr.finaltasks.add(reducebykey.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		assertEquals(5, graph.vertexSet().size());
		assertEquals(0, graph.edgeSet().size());
	}

	@Test
	public void testGeneratePhysicalExecutionPlanMapFilterMapPairReduceByKeyCoalesce() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter = mdparr
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair = filter.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);
		MapPair<String, Long> coalesce = reducebykey.coalesce(1, (a, b) -> a + b);
		mdparr.finaltasks.add(coalesce.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
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
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter = mdparr
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair = filter.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);
		MapPair<String, Long> coalesce = reducebykey.coalesce(1, (a, b) -> a + b);
		mdparr.finaltasks.add(coalesce.task);
		mdparr.finaltasks.add(reducebykey.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		assertEquals(6, graph.vertexSet().size());
		assertEquals(5, graph.edgeSet().size());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testGeneratePhysicalExecutionPlanJoin() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter = mdparr
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair = filter.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);

		MassiveDataPipeline<String> mdp1 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr1 = mdp1.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter1 = mdparr1
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair1 = filter1.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey1 = mappair1.reduceByKey((a, b) -> a + b);
		MapPair<Tuple2, Tuple2> join = reducebykey.join(reducebykey1, (tup1, tup2) -> tup1.v1.equals(tup2.v1));

		((MassiveDataPipeline) join.root).finaltasks.add(join.task);
		((MassiveDataPipeline) join.root).mdsroots.add(mdp);
		Job job = ((MassiveDataPipeline) join.root).createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		assertEquals(3, graph.vertexSet().size());
		assertEquals(2, graph.edgeSet().size());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testGeneratePhysicalExecutionPlanJoinPartitioned() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter = mdparr
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair = filter.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);

		MassiveDataPipeline<String> mdp1 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr1 = mdp1.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter1 = mdparr1
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair1 = filter1.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey1 = mappair1.reduceByKey((a, b) -> a + b);
		MapPair<Tuple2, Tuple2> join = reducebykey.join(reducebykey1, (tup1, tup2) -> tup1.v1.equals(tup2.v1));

		((MassiveDataPipeline) join.root).finaltasks.add(join.task);
		((MassiveDataPipeline) join.root).mdsroots.add(mdp);
		Job job = ((MassiveDataPipeline) join.root).createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		assertEquals(35, graph.vertexSet().size());
		assertEquals(50, graph.edgeSet().size());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testGeneratePhysicalExecutionPlanLeftJoin() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter = mdparr
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair = filter.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);

		MassiveDataPipeline<String> mdp1 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr1 = mdp1.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter1 = mdparr1
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair1 = filter1.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey1 = mappair1.reduceByKey((a, b) -> a + b);
		MapPair<String, Long> join = reducebykey.leftOuterjoin(reducebykey1, (tup1, tup2) -> tup1.v1.equals(tup2.v1));

		((MassiveDataPipeline) join.root).finaltasks.add(join.task);
		((MassiveDataPipeline) join.root).mdsroots.add(mdp);
		Job job = ((MassiveDataPipeline) join.root).createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		assertEquals(3, graph.vertexSet().size());
		assertEquals(2, graph.edgeSet().size());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testGeneratePhysicalExecutionPlanLeftJoinPartitioned() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter = mdparr
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair = filter.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);

		MassiveDataPipeline<String> mdp1 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr1 = mdp1.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter1 = mdparr1
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair1 = filter1.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey1 = mappair1.reduceByKey((a, b) -> a + b);
		MapPair<String, Long> join = reducebykey.leftOuterjoin(reducebykey1, (tup1, tup2) -> tup1.v1.equals(tup2.v1));

		((MassiveDataPipeline) join.root).finaltasks.add(join.task);
		((MassiveDataPipeline) join.root).mdsroots.add(mdp);

		Job job = ((MassiveDataPipeline) join.root).createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		assertEquals(35, graph.vertexSet().size());
		assertEquals(50, graph.edgeSet().size());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testGeneratePhysicalExecutionPlanRightJoin() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter = mdparr
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair = filter.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);

		MassiveDataPipeline<String> mdp1 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr1 = mdp1.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter1 = mdparr1
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair1 = filter1.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey1 = mappair1.reduceByKey((a, b) -> a + b);
		MapPair<String, Long> join = reducebykey.rightOuterjoin(reducebykey1, (tup1, tup2) -> tup1.v1.equals(tup2.v1));

		((MassiveDataPipeline) join.root).finaltasks.add(join.task);
		((MassiveDataPipeline) join.root).mdsroots.add(mdp);

		Job job = ((MassiveDataPipeline) join.root).createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		assertEquals(3, graph.vertexSet().size());
		assertEquals(2, graph.edgeSet().size());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testGeneratePhysicalExecutionPlanRightJoinPartitioned() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter = mdparr
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair = filter.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey = mappair.reduceByKey((a, b) -> a + b);

		MassiveDataPipeline<String> mdp1 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr1 = mdp1.map((val) -> val.split(MDCConstants.COMMA));
		MassiveDataPipeline<String[]> filter1 = mdparr1
				.filter((val) -> !val[14].equals("ArrDelay") && !val[14].equals("NA"));
		MapPair<String, Long> mappair1 = filter1.mapToPair((val) -> Tuple.tuple(val[8], Long.parseLong(val[14])));
		MapPair<String, Long> reducebykey1 = mappair1.reduceByKey((a, b) -> a + b);
		MapPair<String, Long> join = reducebykey.rightOuterjoin(reducebykey1, (tup1, tup2) -> tup1.v1.equals(tup2.v1));

		((MassiveDataPipeline) join.root).finaltasks.add(join.task);
		((MassiveDataPipeline) join.root).mdsroots.add(mdp);

		Job job = ((MassiveDataPipeline) join.root).createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		assertEquals(35, graph.vertexSet().size());
		assertEquals(50, graph.edgeSet().size());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testGeneratePhysicalExecutionPlanUnion() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);

		MassiveDataPipeline<String> mdp1 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);

		MassiveDataPipeline<String> union = mdp.filter((val) -> true).union(mdp1.filter((val) -> true));

		((MassiveDataPipeline) union.root).finaltasks.add(union.task);
		((MassiveDataPipeline) union.root).mdsroots.add(mdp);
		((MassiveDataPipeline) union.root).mdsroots.add(mdp1);

		Job job = ((MassiveDataPipeline) union.root).createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		assertEquals(3, graph.vertexSet().size());
		assertEquals(2, graph.edgeSet().size());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testGeneratePhysicalExecutionPlanUnionPartitioned() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String> mdp1 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String> union = mdp.filter((val) -> true).union(mdp1.filter((val) -> true));

		((MassiveDataPipeline) union.root).finaltasks.add(union.task);
		((MassiveDataPipeline) union.root).mdsroots.add(mdp);
		Job job = ((MassiveDataPipeline) union.root).createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		assertEquals(35, graph.vertexSet().size());
		assertEquals(50, graph.edgeSet().size());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testGeneratePhysicalExecutionPlanIntersection() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);

		MassiveDataPipeline<String> mdp1 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);

		MassiveDataPipeline<String> intersection = mdp.filter((val) -> true).intersection(mdp1.filter((val) -> true));

		((MassiveDataPipeline) intersection.root).finaltasks.add(intersection.task);
		((MassiveDataPipeline) intersection.root).mdsroots.add(mdp);
		Job job = ((MassiveDataPipeline) intersection.root).createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		assertEquals(3, graph.vertexSet().size());
		assertEquals(2, graph.edgeSet().size());
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testGeneratePhysicalExecutionPlanIntersectionPartitioned() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("true");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String> mdp1 = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String> intersection = mdp.filter((val) -> true).intersection(mdp1.filter((val) -> true));

		((MassiveDataPipeline) intersection.root).finaltasks.add(intersection.task);
		((MassiveDataPipeline) intersection.root).mdsroots.add(mdp);
		Job job = ((MassiveDataPipeline) intersection.root).createJob();
		JobScheduler js = new JobScheduler();
		job.pipelineconfig = pc;
		js.isignite = Objects.isNull(pc.getMode()) ? false
				: pc.getMode().equals(MDCConstants.MODE_DEFAULT) ? true : false;
		js.job = job;
		js.pipelineconfig = pc;
		js.semaphore = new Semaphore(1);
		js.resultstream = new ConcurrentHashMap<>();
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		assertEquals(35, graph.vertexSet().size());
		assertEquals(50, graph.edgeSet().size());
	}

	@SuppressWarnings({ "rawtypes", "unchecked", "resource" })
	@Test
	public void testParallelExecutionPhaseDExecutor() throws Exception {

		PipelineConfig pc = new PipelineConfig();
		pc.setMode(MDCConstants.MODE_NORMAL);
		pc.setOutput(new Output(System.out));
		pc.setLocal("false");
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("1");
		pc.setGctype(MDCConstants.ZGC);
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val) -> val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);

		Job job = mdparr.createJob();
		JobScheduler js = new JobScheduler();
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
				job.id);
		// Start the heart beat to receive task executor to task
		// schedulers task status updates.
		js.hbtss.start();
		js.hbtss.getHbo().start();
		HeartBeatServerStream hbss = new HeartBeatServerStream();
		js.hbss = hbss;
		js.getContainersHostPort();
		js.hbss = hbss;
		List<Stage> uniquestagestoprocess = new ArrayList<>(job.topostages);
		int stagenumber = 0;
		SimpleDirectedGraph<MassiveDataStreamTaskSchedulerThread, DAGEdge> graph = new SimpleDirectedGraph<>(
				DAGEdge.class);
		SimpleDirectedGraph<Task, DAGEdge> taskgraph = new SimpleDirectedGraph<>(DAGEdge.class);
		// Generate Physical execution plan for each stages.
		for (Stage stage : uniquestagestoprocess) {
			JobStage jobstage = new JobStage();
			jobstage.jobid = job.id;
			jobstage.stageid = stage.id;
			jobstage.stage = stage;
			js.jsidjsmap.put(job.id + stage.id, jobstage);
			Stage nextstage = stagenumber + 1 < uniquestagestoprocess.size()
					? uniquestagestoprocess.get(stagenumber + 1)
					: null;
			stage.number = stagenumber;
			js.generatePhysicalExecutionPlan(stage, nextstage, job.stageoutputmap, job.id, graph, taskgraph);
			stagenumber++;
		}
		js.broadcastJobStageToTaskExecutors();
		js.parallelExecutionPhaseDExecutor(graph);
		Iterator<MassiveDataStreamTaskSchedulerThread> topostages = new TopologicalOrderIterator(graph);
		List<MassiveDataStreamTaskSchedulerThread> mdststs = new ArrayList<>();
		// Sequential ordering of topological ordering is obtained to
		// process for parallelization.
		while (topostages.hasNext())
			mdststs.add(topostages.next());
		js.parallelExecutionPhaseDExecutorLocalMode(graph, js.new TaskProviderLocalMode(graph.vertexSet().size()));
		List<List> results = js.getLastStageOutput(graph, mdststs, false, false, false, false, js.resultstream);
		int sum = 0;
		for (List result : results) {
			sum += result.size();
		}
		assertEquals(46361, sum);
		js.hbtss.getHbo().stop();
		js.hbtss.close();
		js.hbss.stop();
		js.hbss.destroy();
		js.destroyContainers();
	}

}