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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.jooq.lambda.tuple.Tuple2;
import org.json.simple.JSONObject;
import org.junit.Test;
import org.xerial.snappy.SnappyInputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.HDFSBlockUtils;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCCache;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.Stage;
import com.github.mdc.common.Task;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.CsvOptions;
import com.github.mdc.stream.Json;
import com.github.mdc.stream.StreamPipelineTestCommon;
import com.github.mdc.common.functions.CalculateCount;
import com.github.mdc.common.functions.Coalesce;
import com.github.mdc.common.functions.CountByKeyFunction;
import com.github.mdc.common.functions.FoldByKey;
import com.github.mdc.common.functions.IntersectionFunction;
import com.github.mdc.common.functions.JoinPredicate;
import com.github.mdc.common.functions.LeftOuterJoinPredicate;
import com.github.mdc.common.functions.MapFunction;
import com.github.mdc.common.functions.MapToPairFunction;
import com.github.mdc.common.functions.Max;
import com.github.mdc.common.functions.Min;
import com.github.mdc.common.functions.PredicateSerializable;
import com.github.mdc.common.functions.ReduceByKeyFunction;
import com.github.mdc.common.functions.RightOuterJoinPredicate;
import com.github.mdc.common.functions.StandardDeviation;
import com.github.mdc.common.functions.Sum;
import com.github.mdc.common.functions.SummaryStatistics;
import com.github.mdc.common.functions.UnionFunction;
import com.github.mdc.stream.utils.FileBlocksPartitionerHDFS;

public class StreamPipelineTaskExecutorJGroupsTest extends StreamPipelineTestCommon {
	Map<String, JobStage> jsidjsmap = new ConcurrentHashMap<>();

	// CSV Test Cases Start
	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSIntersection() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		Object function = new IntersectionFunction();
		js.stage.tasks.add(function);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(task), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls, false);
		mdsjte.setTask(task);
		mdsjte.processBlockHDFSIntersection(bls.get(0), bls.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(task);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<String> intersectiondata = (List<String>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(46361, intersectiondata.size());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSIntersectionDiff() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		Object function = new IntersectionFunction();
		js.stage.tasks.add(function);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(task), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths2) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, true,
				128 * MDCConstants.MB);
		fbp.getDnXref(bls2, false);
		mdsjte.setTask(task);
		mdsjte.processBlockHDFSIntersection(bls1.get(0), bls2.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(task);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<String> intersectiondata = (List<String>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(20, intersectiondata.size());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSIntersectionDiff() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		Object function = new IntersectionFunction();
		js.stage.tasks.add(function);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(task), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths2) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, true,
				128 * MDCConstants.MB);
		fbp.getDnXref(bls2, false);
		mdsjte.setTask(task);
		mdsjte.processBlockHDFSIntersection(bls1.get(0), bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(task);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		Set<InputStream> istreams = new LinkedHashSet<>(Arrays.asList(fsdis));
		task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		function = new IntersectionFunction();
		js.stage.tasks.clear();
		js.stage.tasks.add(function);
		mdsjte.setTask(task);
		mdsjte.processBlockHDFSIntersection(istreams, Arrays.asList(bls2.get(0)), hdfs);
		fsdis.close();
		mdsjte.setTask(task);
		path = mdsjte.getIntermediateDataFSFilePath(task);
		fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<String> intersectiondata = (List<String>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(20, intersectiondata.size());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamsBlockHDFSIntersectionDiff() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task task1 = new Task();
		task1.jobid = js.jobid;
		task1.stageid = js.stageid;
		Object function = new IntersectionFunction();
		js.stage.tasks.add(function);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(task1), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths2) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, true,
				128 * MDCConstants.MB);
		fbp.getDnXref(bls2, false);
		mdsjte.setTask(task1);
		mdsjte.processBlockHDFSIntersection(bls1.get(0), bls1.get(0), hdfs);
		Task task2 = new Task();
		task2.jobid = js.jobid;
		task2.stageid = js.stageid;
		function = new IntersectionFunction();
		js.stage.tasks.clear();
		js.stage.tasks.add(function);
		mdsjte.setTask(task2);
		mdsjte.processBlockHDFSIntersection(bls2.get(0), bls2.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(task1);
		InputStream fsdis1 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<InputStream> istreams1 = Arrays.asList(fsdis1);
		path = mdsjte.getIntermediateDataFSFilePath(task2);
		InputStream fsdis2 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<InputStream> istreams2 = Arrays.asList(fsdis2);

		Task taskinter = new Task();
		taskinter.jobid = js.jobid;
		taskinter.stageid = js.stageid;
		function = new IntersectionFunction();
		js.stage.tasks.clear();
		js.stage.tasks.add(function);
		mdsjte.setTask(taskinter);
		mdsjte.processBlockHDFSIntersection(istreams1, istreams2);
		fsdis1.close();
		fsdis2.close();

		path = mdsjte.getIntermediateDataFSFilePath(taskinter);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<String> intersectiondata = (List<String>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(20, intersectiondata.size());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSUnion() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		Object function = new UnionFunction();
		js.stage.tasks.add(function);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(task), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls, false);
		mdsjte.setTask(task);
		mdsjte.processBlockHDFSUnion(bls.get(0), bls.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(task);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<String> uniondata = (List<String>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(46361, uniondata.size());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSUnionDiff() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		Object function = new UnionFunction();
		js.stage.tasks.add(function);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(task), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths3) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths4) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, true,
				128 * MDCConstants.MB);
		fbp.getDnXref(bls2, false);
		mdsjte.setTask(task);
		mdsjte.processBlockHDFSUnion(bls1.get(0), bls2.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(task);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<String> uniondata = (List<String>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(60, uniondata.size());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSUnionDiff() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		Object function = new UnionFunction();
		js.stage.tasks.add(task);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(task), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths3) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths4) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, true,
				128 * MDCConstants.MB);
		fbp.getDnXref(bls2, false);
		mdsjte.setTask(task);
		mdsjte.processBlockHDFSUnion(bls1.get(0), bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(task);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		Set<InputStream> istreams = new LinkedHashSet<>(Arrays.asList(fsdis));
		task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		function = new UnionFunction();
		js.stage.tasks.clear();
		js.stage.tasks.add(function);
		mdsjte.setTask(task);
		mdsjte.processBlockHDFSUnion(istreams, Arrays.asList(bls2.get(0)), hdfs);
		fsdis.close();
		path = mdsjte.getIntermediateDataFSFilePath(task);
		fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<String> uniondata = (List<String>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(60, uniondata.size());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamsBlockHDFSUnionDiff() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task task1 = new Task();
		task1.jobid = js.jobid;
		task1.stageid = js.stageid;
		Object function = new UnionFunction();
		js.stage.tasks.add(function);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(task1), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths3) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths4) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, true,
				128 * MDCConstants.MB);
		fbp.getDnXref(bls2, false);
		mdsjte.setTask(task1);
		mdsjte.processBlockHDFSUnion(bls1.get(0), bls1.get(0), hdfs);
		Task task2 = new Task();
		task2.jobid = js.jobid;
		task2.stageid = js.stageid;
		function = new UnionFunction();
		js.stage.tasks.clear();
		js.stage.tasks.add(function);
		mdsjte.setTask(task2);
		mdsjte.processBlockHDFSUnion(bls2.get(0), bls2.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(task1);
		InputStream fsdis1 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<InputStream> istreams1 = Arrays.asList(fsdis1);

		path = mdsjte.getIntermediateDataFSFilePath(task2);
		InputStream fsdis2 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<InputStream> istreams2 = Arrays.asList(fsdis2);

		Task taskunion = new Task();
		taskunion.jobid = js.jobid;
		taskunion.stageid = js.stageid;
		function = new UnionFunction();
		js.stage.tasks.clear();
		js.stage.tasks.add(function);
		mdsjte.setTask(taskunion);
		mdsjte.processBlockHDFSUnion(istreams1, istreams2);
		fsdis1.close();
		fsdis2.close();

		path = mdsjte.getIntermediateDataFSFilePath(taskunion);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<String> uniondata = (List<String>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(60, uniondata.size());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMap() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(filtertask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(filtertask);

		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<String[]> mapfilterdata = (List<String[]>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(45957, mapfilterdata.size());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCount() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task calculatecounttask = new Task();
		calculatecounttask.jobid = js.jobid;
		calculatecounttask.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(new CalculateCount());

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(calculatecounttask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(calculatecounttask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(calculatecounttask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Long> mapfiltercountdata = (List<Long>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(45957l, (long) mapfiltercountdata.get(0));
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapSummaryStatistics() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task sstask = new Task();
		sstask.jobid = js.jobid;
		sstask.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		ToIntFunction<String[]> toint = (String str[]) -> Integer.parseInt(str[14]);
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(toint);
		js.stage.tasks.add(new SummaryStatistics());

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(sstask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(sstask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsjte.getIntermediateDataFSFilePath(sstask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		List<IntSummaryStatistics> mapfilterssdata = (List<IntSummaryStatistics>) kryo
				.readClassAndObject(new Input(fsdis));
		assertEquals(1, (long) mapfilterssdata.size());
		assertEquals(623, (long) mapfilterssdata.get(0).getMax());
		assertEquals(-89, (long) mapfilterssdata.get(0).getMin());
		assertEquals(-63278, (long) mapfilterssdata.get(0).getSum());
		assertEquals(45957l, mapfilterssdata.get(0).getCount());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapMax() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();

		Task maxtask = new Task();
		maxtask.jobid = js.jobid;
		maxtask.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		ToIntFunction<String[]> toint = (String str[]) -> Integer.parseInt(str[14]);
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(toint);
		js.stage.tasks.add(new Max());

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(maxtask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(maxtask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(maxtask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Integer> mapfiltermaxdata = (List<Integer>) kryo.readClassAndObject(new Input(fsdis));
		fsdis.close();
		assertEquals(623, (int) mapfiltermaxdata.get(0));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapMin() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task mintask = new Task();
		mintask.jobid = js.jobid;
		mintask.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		ToIntFunction<String[]> toint = (String str[]) -> Integer.parseInt(str[14]);
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(toint);
		js.stage.tasks.add(new Min());

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(mintask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(mintask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(mintask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Integer> mapfiltermaxdata = (List<Integer>) kryo.readClassAndObject(new Input(fsdis));
		fsdis.close();
		assertEquals(-89, (int) mapfiltermaxdata.get(0));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapSum() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task sumtask = new Task();
		sumtask.jobid = js.jobid;
		sumtask.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		ToIntFunction<String[]> toint = (String str[]) -> Integer.parseInt(str[14]);
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(toint);
		js.stage.tasks.add(new Sum());

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(sumtask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(sumtask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(sumtask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Integer> mapfiltermaxdata = (List<Integer>) kryo.readClassAndObject(new Input(fsdis));
		fsdis.close();
		assertEquals(-63278, (int) mapfiltermaxdata.get(0));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapSD() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task sdtask = new Task();
		sdtask.jobid = js.jobid;
		sdtask.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		ToIntFunction<String[]> toint = (String str[]) -> Integer.parseInt(str[14]);
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(toint);
		js.stage.tasks.add(new StandardDeviation());

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(sdtask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(sdtask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(sdtask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Double> mapfiltermaxdata = (List<Double>) kryo.readClassAndObject(new Input(fsdis));
		fsdis.close();
		assertEquals(1, mapfiltermaxdata.size());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVCount() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task calcultecounttask = new Task();
		calcultecounttask.jobid = js.jobid;
		calcultecounttask.stageid = js.stageid;
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(new CalculateCount());

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(calcultecounttask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(calcultecounttask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(calcultecounttask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Long> mapfiltercountdata = (List<Long>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(45957l, (long) mapfiltercountdata.get(0));
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecord() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get("ArrDelay"))
				&& !"NA".equals(csvrecord.get("ArrDelay"));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(filtertask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(filtertask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<CSVRecord> filterdata = (List<CSVRecord>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(45957l, (long) filterdata.size());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordSumaryStatistics() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task summarystaticstask = new Task();
		summarystaticstask.jobid = js.jobid;
		summarystaticstask.stageid = js.stageid;
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get("ArrDelay"))
				&& !"NA".equals(csvrecord.get("ArrDelay"));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get("ArrDelay"));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(csvint);
		js.stage.tasks.add(new SummaryStatistics());

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(summarystaticstask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(summarystaticstask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(summarystaticstask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<IntSummaryStatistics> mapfilterssdata = (List<IntSummaryStatistics>) kryo
				.readClassAndObject(new Input(fsdis));
		assertEquals(1, (long) mapfilterssdata.size());
		assertEquals(623, (long) mapfilterssdata.get(0).getMax());
		assertEquals(-89, (long) mapfilterssdata.get(0).getMin());
		assertEquals(-63278, (long) mapfilterssdata.get(0).getSum());
		assertEquals(45957l, mapfilterssdata.get(0).getCount());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordMax() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task maxtask = new Task();
		maxtask.jobid = js.jobid;
		maxtask.stageid = js.stageid;
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get("ArrDelay"))
				&& !"NA".equals(csvrecord.get("ArrDelay"));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get("ArrDelay"));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(csvint);
		js.stage.tasks.add(new Max());

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(maxtask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(maxtask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(maxtask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Integer> mapfilterssdata = (List<Integer>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(623, (long) mapfilterssdata.get(0));
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordMin() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task mintask = new Task();
		mintask.jobid = js.jobid;
		mintask.stageid = js.stageid;
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get("ArrDelay"))
				&& !"NA".equals(csvrecord.get("ArrDelay"));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get("ArrDelay"));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(csvint);
		js.stage.tasks.add(new Min());

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(mintask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(mintask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(mintask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Integer> mapfilterssdata = (List<Integer>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(-89, (long) mapfilterssdata.get(0));
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordSum() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task sumtask = new Task();
		sumtask.jobid = js.jobid;
		sumtask.stageid = js.stageid;
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get("ArrDelay"))
				&& !"NA".equals(csvrecord.get("ArrDelay"));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get("ArrDelay"));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(csvint);
		js.stage.tasks.add(new Sum());

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(sumtask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(sumtask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(sumtask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Integer> mapfilterssdata = (List<Integer>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(-63278, (long) mapfilterssdata.get(0));
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordStandardDeviation() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task sdtask = new Task();
		sdtask.jobid = js.jobid;
		sdtask.stageid = js.stageid;
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get("ArrDelay"))
				&& !"NA".equals(csvrecord.get("ArrDelay"));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get("ArrDelay"));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(csvint);
		js.stage.tasks.add(new StandardDeviation());

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(sdtask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(sdtask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(sdtask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Double> mapfiltersddata = (List<Double>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(1, (long) mapfiltersddata.size());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVCount() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(filtertask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(filtertask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsjte.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(fsdis));
		Task calcultecounttask = new Task();
		calcultecounttask.jobid = js.jobid;
		calcultecounttask.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(new CalculateCount());
		mdsjte.setTask(calcultecounttask);
		mdsjte.processBlockHDFSMap(inputtocount);
		fsdis.close();
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		path = mdsjte.getIntermediateDataFSFilePath(calcultecounttask);
		fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Long> csvreccount = (List<Long>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(45957l, (long) csvreccount.get(0));
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVSummaryStatistics() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(filtertask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(filtertask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsjte.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(fsdis));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get("ArrDelay"));
		Task summarystaticstask = new Task();
		summarystaticstask.jobid = js.jobid;
		summarystaticstask.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(csvint);
		js.stage.tasks.add(new SummaryStatistics());
		mdsjte.setTask(summarystaticstask);
		mdsjte.processBlockHDFSMap(inputtocount);
		fsdis.close();
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		path = mdsjte.getIntermediateDataFSFilePath(summarystaticstask);
		fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<IntSummaryStatistics> mapfilterssdata = (List<IntSummaryStatistics>) kryo
				.readClassAndObject(new Input(fsdis));
		assertEquals(1, (long) mapfilterssdata.size());
		assertEquals(623, (long) mapfilterssdata.get(0).getMax());
		assertEquals(-89, (long) mapfilterssdata.get(0).getMin());
		assertEquals(-63278, (long) mapfilterssdata.get(0).getSum());
		assertEquals(45957l, mapfilterssdata.get(0).getCount());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVMax() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(filtertask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(filtertask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsjte.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(fsdis));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get("ArrDelay"));
		Task maxtask = new Task();
		maxtask.jobid = js.jobid;
		maxtask.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(csvint);
		js.stage.tasks.add(new Max());
		mdsjte.setTask(maxtask);
		mdsjte.processBlockHDFSMap(inputtocount);
		fsdis.close();
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		path = mdsjte.getIntermediateDataFSFilePath(maxtask);
		fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Integer> mapfiltermaxdata = (List<Integer>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(623, (int) mapfiltermaxdata.get(0));
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVMin() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(filtertask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(filtertask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsjte.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(fsdis));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get("ArrDelay"));
		Task mintask = new Task();
		mintask.jobid = js.jobid;
		mintask.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(csvint);
		js.stage.tasks.add(new Min());
		mdsjte.setTask(mintask);
		mdsjte.processBlockHDFSMap(inputtocount);
		fsdis.close();
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		path = mdsjte.getIntermediateDataFSFilePath(mintask);
		fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Integer> mapfiltermindata = (List<Integer>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(-89, (int) mapfiltermindata.get(0));
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVSum() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(filtertask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(filtertask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsjte.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(fsdis));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get("ArrDelay"));
		Task sumtask = new Task();
		sumtask.jobid = js.jobid;
		sumtask.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(csvint);
		js.stage.tasks.add(new Sum());
		mdsjte.setTask(sumtask);
		mdsjte.processBlockHDFSMap(inputtocount);
		fsdis.close();
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		path = mdsjte.getIntermediateDataFSFilePath(sumtask);
		fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Integer> mapfiltersumdata = (List<Integer>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(-63278, (int) mapfiltersumdata.get(0));
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVSD() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(filtertask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(filtertask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsjte.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(fsdis));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get("ArrDelay"));
		Task sdtask = new Task();
		sdtask.jobid = js.jobid;
		sdtask.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(csvint);
		js.stage.tasks.add(new StandardDeviation());
		mdsjte.setTask(sdtask);
		mdsjte.processBlockHDFSMap(inputtocount);
		fsdis.close();
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		path = mdsjte.getIntermediateDataFSFilePath(sdtask);
		fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Integer> mapfiltersddata = (List<Integer>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(1, (int) mapfiltersddata.size());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessSample() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(filtertask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(filtertask);
		mdsjte.processSamplesBlocks(100, bls1.get(0), hdfs);
		String path = mdsjte.getIntermediateDataFSFilePath(filtertask);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Long> csvreccount = (List<Long>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(100, (long) csvreccount.size());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessSampleCount() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task counttask = new Task();
		counttask.jobid = js.jobid;
		counttask.stageid = js.stageid;
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(new CalculateCount());
		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(counttask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(counttask);
		mdsjte.processSamplesBlocks(150, bls1.get(0), hdfs);
		String path = mdsjte.getIntermediateDataFSFilePath(counttask);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Long> csvreccount = (List<Long>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(150l, (long) csvreccount.get(0));
		fsdis.close();
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessStreamSample() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(filtertask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(filtertask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsjte.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<InputStream> inputtocount = Arrays.asList(fsdis);
		Task sample = new Task();
		sample.jobid = js.jobid;
		sample.stageid = js.stageid;
		Function samplefn = val -> val;
		js.stage.tasks.clear();
		js.stage.tasks.add(samplefn);
		mdsjte.setTask(sample);
		mdsjte.processSamplesObjects(150, inputtocount);
		fsdis.close();
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		path = mdsjte.getIntermediateDataFSFilePath(sample);
		fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Integer> mapfiltersddata = (List<Integer>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(150, (int) mapfiltersddata.size());
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamSampleCount() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(filtertask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(filtertask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsjte.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<InputStream> inputtocount = Arrays.asList(fsdis);
		Task count = new Task();
		count.jobid = js.jobid;
		count.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(new CalculateCount());
		mdsjte.setTask(count);
		mdsjte.processSamplesObjects(150, inputtocount);
		fsdis.close();
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		path = mdsjte.getIntermediateDataFSFilePath(count);
		fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Long> mapfiltersddata = (List<Long>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(150l, (long) mapfiltersddata.get(0));
		fsdis.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessJoin() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task reducebykeytask1 = new Task();
		reducebykeytask1.jobid = js.jobid;
		reducebykeytask1.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Integer>> pair = val -> new Tuple2<String, Integer>(
				(String) val[8], (Integer) Integer.parseInt(val[14]));
		ReduceByKeyFunction<Integer> redfunc = (input1, input2) -> input1 + input2;
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);
		js.stage.tasks.add(redfunc);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(reducebykeytask1), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, true,
				128 * MDCConstants.MB);
		fbp.getDnXref(bls2, false);
		mdsjte.setTask(reducebykeytask1);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		reducebykeytask2.jobid = js.jobid;
		reducebykeytask2.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);
		js.stage.tasks.add(redfunc);
		mdsjte.setTask(reducebykeytask2);
		mdsjte.processBlockHDFSMap(bls2.get(0), hdfs);

		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path1 = mdsjte.getIntermediateDataFSFilePath(reducebykeytask1);
		InputStream fsdis1 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 )));
		String path2 = mdsjte.getIntermediateDataFSFilePath(reducebykeytask2);
		InputStream fsdis2 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path2 )));
		Task jointask = new Task();
		jointask.jobid = js.jobid;
		jointask.stageid = js.stageid;
		js.stage.tasks.clear();
		Consumer<String> dummy = val -> {
		};
		js.stage.tasks.add(dummy);
		JoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdsjte.setTask(jointask);
		mdsjte.processJoinLZF(fsdis1, fsdis2, jp, false, false);
		fsdis1.close();
		fsdis2.close();

		kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(jointask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> mapfiltersddata = (List) kryo
				.readClassAndObject(new Input(fsdis));
		assertEquals(1, (long) mapfiltersddata.size());
		Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>> tupleresult = mapfiltersddata.get(0);
		assertEquals(tupleresult.v1.v1, tupleresult.v2.v1);
		assertEquals(tupleresult.v1.v2, tupleresult.v2.v2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessLeftOuterJoin() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task reducebykeytask1 = new Task();
		reducebykeytask1.jobid = js.jobid;
		reducebykeytask1.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Integer>> pair = val -> new Tuple2<String, Integer>(
				(String) val[8], (Integer) Integer.parseInt(val[14]));
		ReduceByKeyFunction<Integer> redfunc = (input1, input2) -> input1 + input2;
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);
		js.stage.tasks.add(redfunc);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(reducebykeytask1), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths2) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, true,
				128 * MDCConstants.MB);
		fbp.getDnXref(bls2, false);
		mdsjte.setTask(reducebykeytask1);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		reducebykeytask2.jobid = js.jobid;
		reducebykeytask2.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);
		js.stage.tasks.add(redfunc);
		mdsjte.setTask(reducebykeytask2);
		mdsjte.processBlockHDFSMap(bls2.get(0), hdfs);

		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path1 = mdsjte.getIntermediateDataFSFilePath(reducebykeytask1);
		InputStream fsdis1 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 )));
		String path2 = mdsjte.getIntermediateDataFSFilePath(reducebykeytask2);
		InputStream fsdis2 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path2 )));
		Task jointask = new Task();
		jointask.jobid = js.jobid;
		jointask.stageid = js.stageid;
		js.stage.tasks.clear();
		Consumer<String> dummy = val -> {
		};
		js.stage.tasks.add(dummy);

		LeftOuterJoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdsjte.setTask(jointask);
		mdsjte.processLeftOuterJoinLZF(fsdis1, fsdis2, jp, false, false);
		fsdis1.close();
		fsdis2.close();

		kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(jointask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> mapfiltersddata = (List) kryo
				.readClassAndObject(new Input(fsdis));
		assertEquals(1, (long) mapfiltersddata.size());
		Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>> tupleresult = mapfiltersddata.get(0);
		assertNotNull(tupleresult.v1.v1);
		assertNotNull(tupleresult.v2.v1);
		assertEquals(-63278, (int) tupleresult.v1.v2);
		assertEquals("AQ", tupleresult.v1.v1);
		assertEquals(10, (int) tupleresult.v2.v2);
		assertEquals("AQ", tupleresult.v2.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessRightOuterJoin() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task reducebykeytask1 = new Task();
		reducebykeytask1.jobid = js.jobid;
		reducebykeytask1.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Integer>> pair = val -> new Tuple2<String, Integer>(
				(String) val[8], (Integer) Integer.parseInt(val[14]));
		ReduceByKeyFunction<Integer> redfunc = (input1, input2) -> input1 + input2;
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);
		js.stage.tasks.add(redfunc);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(reducebykeytask1), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths2) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, true,
				128 * MDCConstants.MB);
		fbp.getDnXref(bls2, false);
		mdsjte.setTask(reducebykeytask1);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		reducebykeytask2.jobid = js.jobid;
		reducebykeytask2.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);
		js.stage.tasks.add(redfunc);
		mdsjte.setTask(reducebykeytask2);
		mdsjte.processBlockHDFSMap(bls2.get(0), hdfs);

		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path1 = mdsjte.getIntermediateDataFSFilePath(reducebykeytask1);
		InputStream fsdis1 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 )));
		String path2 = mdsjte.getIntermediateDataFSFilePath(reducebykeytask2);
		InputStream fsdis2 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path2 )));
		Task jointask = new Task();
		jointask.jobid = js.jobid;
		jointask.stageid = js.stageid;
		js.stage.tasks.clear();
		Consumer<String> dummy = val -> {
		};
		js.stage.tasks.add(dummy);

		RightOuterJoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdsjte.setTask(jointask);
		mdsjte.processRightOuterJoinLZF(fsdis2, fsdis1, jp, false, false);
		fsdis1.close();
		fsdis2.close();

		kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(jointask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> mapfiltersddata = (List) kryo
				.readClassAndObject(new Input(fsdis));
		assertEquals(1, (long) mapfiltersddata.size());
		Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>> tupleresult = mapfiltersddata.get(0);
		assertNotNull(tupleresult.v1.v1);
		assertNotNull(tupleresult.v2.v1);
		assertEquals(10, (int) tupleresult.v1.v2);
		assertEquals("AQ", tupleresult.v1.v1);
		assertEquals(-63278, (int) tupleresult.v2.v2);
		assertEquals("AQ", tupleresult.v2.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessGroupByKey() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task mappairtask1 = new Task();
		mappairtask1.jobid = js.jobid;
		mappairtask1.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Integer>> pair = val -> new Tuple2<String, Integer>(
				(String) val[8], (Integer) Integer.parseInt(val[14]));
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(mappairtask1), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(mappairtask1);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);

		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path1 = mdsjte.getIntermediateDataFSFilePath(mappairtask1);
		InputStream fsdis1 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 )));
		Task gbktask = new Task();
		gbktask.jobid = js.jobid;
		gbktask.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(gbktask);
		Consumer<String> dummy = val -> {
		};
		gbktask.input = new Object[]{fsdis1};
		mdsjte.setTask(gbktask);
		mdsjte.processGroupByKeyTuple2();
		fsdis1.close();

		kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(gbktask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Tuple2<String, List<Integer>>> mapfiltersddata = (List) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(1, (long) mapfiltersddata.size());
		Tuple2<String, List<Integer>> tupleresult = mapfiltersddata.get(0);
		assertEquals(45957l, (int) tupleresult.v2.size());
		assertEquals("AQ", tupleresult.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessFoldLeft() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task mappairtask1 = new Task();
		mappairtask1.jobid = js.jobid;
		mappairtask1.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Long>> pair = val -> new Tuple2<String, Long>((String) val[8],
				(Long) Long.parseLong(val[14]));
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(mappairtask1), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(mappairtask1);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);

		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path1 = mdsjte.getIntermediateDataFSFilePath(mappairtask1);
		InputStream fsdis1 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 )));
		Task fbktask = new Task();
		fbktask.jobid = js.jobid;
		fbktask.stageid = js.stageid;
		js.stage.tasks.clear();
		ReduceByKeyFunction<Long> redfunc = (a, b) -> a + b;
		FoldByKey fbk = new FoldByKey(0l, redfunc, true);
		js.stage.tasks.add(fbk);
		fbktask.input = new Object[]{fsdis1};
		mdsjte.setTask(fbktask);
		mdsjte.processFoldByKeyTuple2();
		fsdis1.close();

		kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(fbktask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Tuple2<String, Long>> mapfiltersddata = (List) kryo.readClassAndObject(new Input(fsdis));

		Tuple2<String, Long> tupleresult = mapfiltersddata.get(0);
		assertEquals(-63278l, (long) tupleresult.v2);
		assertEquals("AQ", tupleresult.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessFoldRight() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task mappairtask1 = new Task();
		mappairtask1.jobid = js.jobid;
		mappairtask1.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Long>> pair = val -> new Tuple2<String, Long>((String) val[8],
				(Long) Long.parseLong(val[14]));
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(mappairtask1), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(mappairtask1);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path1 = mdsjte.getIntermediateDataFSFilePath(mappairtask1);
		InputStream fsdis1 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 )));
		Task fbktask = new Task();
		fbktask.jobid = js.jobid;
		fbktask.stageid = js.stageid;
		ReduceByKeyFunction<Long> redfunc = (a, b) -> a + b;
		FoldByKey fbk = new FoldByKey(0l, redfunc, false);
		js.stage.tasks.clear();
		js.stage.tasks.add(fbk);

		fbktask.input = new Object[]{fsdis1};
		mdsjte.setTask(fbktask);
		mdsjte.processFoldByKeyTuple2();
		fsdis1.close();

		kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(fbktask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Tuple2<String, Long>> mapfiltersddata = (List) kryo.readClassAndObject(new Input(fsdis));

		Tuple2<String, Long> tupleresult = mapfiltersddata.get(0);
		assertEquals(-63278l, (long) tupleresult.v2);
		assertEquals("AQ", tupleresult.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessCountByKey() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task mappairtask1 = new Task();
		mappairtask1.jobid = js.jobid;
		mappairtask1.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Long>> pair = val -> new Tuple2<String, Long>((String) val[8],
				(Long) Long.parseLong(val[14]));
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(mappairtask1), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(mappairtask1);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);

		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path1 = mdsjte.getIntermediateDataFSFilePath(mappairtask1);
		InputStream fsdis1 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 )));
		Task cbktask = new Task();
		cbktask.jobid = js.jobid;
		cbktask.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(new CountByKeyFunction());
		cbktask.input = new Object[]{fsdis1};
		mdsjte.setTask(cbktask);
		mdsjte.processCountByKeyTuple2();
		fsdis1.close();

		kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(cbktask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Tuple2<String, Long>> mapfiltersddata = (List) kryo.readClassAndObject(new Input(fsdis));

		Tuple2<String, Long> tupleresult = mapfiltersddata.get(0);
		assertEquals(45957l, (long) tupleresult.v2);
		assertEquals("AQ", tupleresult.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessCountByValue() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task mappairtask1 = new Task();
		mappairtask1.jobid = js.jobid;
		mappairtask1.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Long>> pair = val -> new Tuple2<String, Long>((String) val[8],
				(Long) Long.parseLong(val[14]));
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(mappairtask1), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(mappairtask1);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path1 = mdsjte.getIntermediateDataFSFilePath(mappairtask1);
		InputStream fsdis1 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 )));
		Task cbktask = new Task();
		cbktask.jobid = js.jobid;
		cbktask.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(new CountByKeyFunction());
		cbktask.input = new Object[]{fsdis1};
		mdsjte.setTask(cbktask);
		mdsjte.processCountByValueTuple2();
		fsdis1.close();

		kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(cbktask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Tuple2<Tuple2<String, Long>, Long>> mapfiltersddata = (List) kryo.readClassAndObject(new Input(fsdis));
		int sum = 0;
		for (Tuple2<Tuple2<String, Long>, Long> tupleresult : mapfiltersddata) {
			sum += tupleresult.v2;
			assertEquals("AQ", tupleresult.v1.v1);
		}
		assertEquals(45957l, (long) sum);

	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessCoalesce() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Task reducebykeytask1 = new Task();
		reducebykeytask1.jobid = js.jobid;
		reducebykeytask1.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Integer>> pair = val -> new Tuple2<String, Integer>(
				(String) val[8], (Integer) Integer.parseInt(val[14]));
		ReduceByKeyFunction<Integer> redfunc = (input1, input2) -> input1 + input2;
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);
		js.stage.tasks.add(redfunc);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(reducebykeytask1), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, true,
				128 * MDCConstants.MB);
		fbp.getDnXref(bls2, false);
		mdsjte.setTask(reducebykeytask1);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		js.stage.tasks.clear();
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);
		js.stage.tasks.add(redfunc);
		mdsjte.setTask(reducebykeytask2);
		mdsjte.processBlockHDFSMap(bls2.get(0), hdfs);

		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path1 = mdsjte.getIntermediateDataFSFilePath(reducebykeytask1);
		InputStream fsdis1 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 )));
		String path2 = mdsjte.getIntermediateDataFSFilePath(reducebykeytask2);
		InputStream fsdis2 = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path2 )));
		reducebykeytask2.input = new Object[]{fsdis1, fsdis2};
		Task coalescetask = new Task();
		coalescetask.jobid = js.jobid;
		coalescetask.stageid = js.stageid;
		js.stage.tasks.clear();
		Coalesce<Integer> coalesce = new Coalesce();
		coalesce.setCoalescepartition(1);
		coalesce.setCoalescefunction((a, b) -> a + b);
		coalescetask.input = new Object[]{fsdis1, fsdis2};
		js.stage.tasks.add(coalesce);
		mdsjte.setTask(coalescetask);
		mdsjte.processCoalesce();
		fsdis1.close();
		fsdis2.close();

		kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(coalescetask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<Tuple2<String, Integer>> mapfiltersddata = (List) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(1, (long) mapfiltersddata.size());
		Tuple2<String, Integer> tupleresult = mapfiltersddata.get(0);
		assertEquals(-63278 + -63278, (int) tupleresult.v2);
		assertEquals("AQ", tupleresult.v1);

	}

	// CSV Test Cases End

	// JSON Test cases Start
	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapJSON() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		jsidjsmap.put(js.jobid + js.stageid, js);
		js.stage.tasks = new ArrayList<>();
		Json json = new Json();
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;
		PredicateSerializable<JSONObject> filter = jsonobj -> jsonobj != null
				&& jsonobj.get("type").equals("CreateEvent");
		js.stage.tasks.add(json);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorJGroups mdsjte = new StreamPipelineTaskExecutorJGroups(jsidjsmap,
				Arrays.asList(filtertask), 10101, MDCCache.get());
		mdsjte.setHdfs(hdfs);
		mdsjte.setExecutor(es);
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : githubevents1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, true,
				128 * MDCConstants.MB);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		mdsjte.setTask(filtertask);
		mdsjte.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoSerializerDeserializer();
		String path = mdsjte.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new SnappyInputStream(new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path )));
		List<JSONObject> jsonfilterdata = (List<JSONObject>) kryo.readClassAndObject(new Input(fsdis));
		assertEquals(11l, (long) jsonfilterdata.size());
		fsdis.close();
	}
	// JSON Test cases End

}
