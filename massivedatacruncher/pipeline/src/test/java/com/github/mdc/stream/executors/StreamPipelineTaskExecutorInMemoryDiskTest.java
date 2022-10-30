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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
import org.nustaq.serialization.FSTObjectInput;

import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.HDFSBlockUtils;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCCache;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.Stage;
import com.github.mdc.common.Task;
import com.github.mdc.common.Utils;
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
import com.github.mdc.stream.CsvOptions;
import com.github.mdc.stream.Json;
import com.github.mdc.stream.StreamPipelineTestCommon;
import com.github.mdc.stream.utils.FileBlocksPartitionerHDFS;

public class StreamPipelineTaskExecutorInMemoryDiskTest extends StreamPipelineTestCommon {
	ConcurrentMap<String, OutputStream> resultstream = new ConcurrentHashMap<>();


	// CSV Test Cases Start
	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSIntersection() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		Object function = new IntersectionFunction();
		js.getStage().tasks.add(function);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(task);
		mdsteim.setExecutor(es);
		mdsteim.processBlockHDFSIntersection(bls.get(0), bls.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(task);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<String> intersectiondata = (List<String>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(46361, intersectiondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSIntersectionDiff() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		Object function = new IntersectionFunction();
		js.getStage().tasks.add(function);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(task);
		mdsteim.setExecutor(es);
		mdsteim.processBlockHDFSIntersection(bls1.get(0), bls2.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(task);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<String> intersectiondata = (List<String>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(20, intersectiondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSIntersectionDiff() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		Object function = new IntersectionFunction();
		js.getStage().tasks.add(function);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(task);
		mdsteim.setExecutor(es);
		mdsteim.processBlockHDFSIntersection(bls1.get(0), bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(task);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		Set<InputStream> istreams = new LinkedHashSet<>(Arrays.asList(is));
		task = new Task();
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		function = new IntersectionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdsteim.setTask(task);
		mdsteim.setExecutor(es);
		mdsteim.processBlockHDFSIntersection(istreams, Arrays.asList(bls2.get(0)), hdfs);
		is.close();
		mdsteim.setTask(task);
		mdsteim.setExecutor(es);
		path = mdsteim.getIntermediateDataFSFilePath(task);
		is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<String> intersectiondata = (List<String>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(20, intersectiondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamsBlockHDFSIntersectionDiff() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task1 = new Task();
		Object function = new IntersectionFunction();
		js.getStage().tasks.add(function);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(task1);
		mdsteim.setExecutor(es);
		mdsteim.processBlockHDFSIntersection(bls1.get(0), bls1.get(0), hdfs);
		Task task2 = new Task();
		task2.jobid = js.getJobid();
		task2.stageid = js.getStageid();
		function = new IntersectionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdsteim.setTask(task2);
		mdsteim.setExecutor(es);
		mdsteim.processBlockHDFSIntersection(bls2.get(0), bls2.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(task1);
		InputStream is1 = new ByteArrayInputStream((byte[]) cache.get(path));
		List<InputStream> istreams1 = Arrays.asList(is1);
		path = mdsteim.getIntermediateDataFSFilePath(task2);
		InputStream is2 = new ByteArrayInputStream((byte[]) cache.get(path));
		List<InputStream> istreams2 = Arrays.asList(is2);

		Task taskinter = new Task();
		taskinter.jobid = js.getJobid();
		taskinter.stageid = js.getStageid();
		function = new IntersectionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdsteim.setTask(taskinter);
		mdsteim.processBlockHDFSIntersection(istreams1, istreams2);
		is1.close();
		is2.close();

		path = mdsteim.getIntermediateDataFSFilePath(taskinter);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<String> intersectiondata = (List<String>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(20, intersectiondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSUnion() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		Object function = new UnionFunction();
		js.getStage().tasks.add(function);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(task);
		mdsteim.setExecutor(es);
		mdsteim.processBlockHDFSUnion(bls.get(0), bls.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(task);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<String> uniondata = (List<String>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(46361, uniondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSUnionDiff() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		Object function = new UnionFunction();
		js.getStage().tasks.add(function);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(task);
		mdsteim.setExecutor(es);
		mdsteim.processBlockHDFSUnion(bls1.get(0), bls2.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(task);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<String> uniondata = (List<String>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(60, uniondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSUnionDiff() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		Object function = new UnionFunction();
		js.getStage().tasks.add(task);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(task);
		mdsteim.setExecutor(es);
		mdsteim.processBlockHDFSUnion(bls1.get(0), bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(task);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		Set<InputStream> istreams = new LinkedHashSet<>(Arrays.asList(is));
		task = new Task();
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		function = new UnionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdsteim.setTask(task);
		mdsteim.setExecutor(es);
		mdsteim.processBlockHDFSUnion(istreams, Arrays.asList(bls2.get(0)), hdfs);
		is.close();
		path = mdsteim.getIntermediateDataFSFilePath(task);
		is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<String> uniondata = (List<String>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(60, uniondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamsBlockHDFSUnionDiff() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task1 = new Task();
		task1.jobid = js.getJobid();
		task1.stageid = js.getStageid();
		Object function = new UnionFunction();
		js.getStage().tasks.add(function);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(task1);
		mdsteim.setExecutor(es);
		mdsteim.processBlockHDFSUnion(bls1.get(0), bls1.get(0), hdfs);
		Task task2 = new Task();
		task2.jobid = js.getJobid();
		task2.stageid = js.getStageid();
		function = new UnionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdsteim.setTask(task2);
		mdsteim.setExecutor(es);
		mdsteim.processBlockHDFSUnion(bls2.get(0), bls2.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(task1);
		InputStream is1 = new ByteArrayInputStream((byte[]) cache.get(path));
		List<InputStream> istreams1 = Arrays.asList(is1);

		path = mdsteim.getIntermediateDataFSFilePath(task2);
		InputStream is2 = new ByteArrayInputStream((byte[]) cache.get(path));
		List<InputStream> istreams2 = Arrays.asList(is2);

		Task taskunion = new Task();
		taskunion.jobid = js.getJobid();
		taskunion.stageid = js.getStageid();
		function = new UnionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdsteim.setTask(taskunion);
		mdsteim.processBlockHDFSUnion(istreams1, istreams2);
		is1.close();
		is2.close();

		path = mdsteim.getIntermediateDataFSFilePath(taskunion);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<String> uniondata = (List<String>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(60, uniondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMap() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task filtertask = new Task();
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(filtertask);

		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(filtertask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<String[]> mapfilterdata = (List<String[]>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(45957, mapfilterdata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCount() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task calculatecounttask = new Task();
		calculatecounttask.jobid = js.getJobid();
		calculatecounttask.stageid = js.getStageid();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(new CalculateCount());

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(calculatecounttask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(calculatecounttask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Long> mapfiltercountdata = (List<Long>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(45957l, (long) mapfiltercountdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapSummaryStatistics() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task sstask = new Task();
		sstask.jobid = js.getJobid();
		sstask.stageid = js.getStageid();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		ToIntFunction<String[]> toint = (String str[]) -> Integer.parseInt(str[14]);
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(toint);
		js.getStage().tasks.add(new SummaryStatistics());

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(sstask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(sstask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<IntSummaryStatistics> mapfilterssdata = (List<IntSummaryStatistics>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(1, (long) mapfilterssdata.size());
		assertEquals(623, (long) mapfilterssdata.get(0).getMax());
		assertEquals(-89, (long) mapfilterssdata.get(0).getMin());
		assertEquals(-63278, (long) mapfilterssdata.get(0).getSum());
		assertEquals(45957l, mapfilterssdata.get(0).getCount());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapMax() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();

		Task maxtask = new Task();
		maxtask.jobid = js.getJobid();
		maxtask.stageid = js.getStageid();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		ToIntFunction<String[]> toint = (String str[]) -> Integer.parseInt(str[14]);
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(toint);
		js.getStage().tasks.add(new Max());

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(maxtask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(maxtask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Integer> mapfiltermaxdata = (List<Integer>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		is.close();
		assertEquals(623, (int) mapfiltermaxdata.get(0));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapMin() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task mintask = new Task();
		mintask.jobid = js.getJobid();
		mintask.stageid = js.getStageid();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		ToIntFunction<String[]> toint = (String str[]) -> Integer.parseInt(str[14]);
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(toint);
		js.getStage().tasks.add(new Min());

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(mintask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(mintask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Integer> mapfiltermaxdata = (List<Integer>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		is.close();
		assertEquals(-89, (int) mapfiltermaxdata.get(0));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapSum() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task sumtask = new Task();
		sumtask.jobid = js.getJobid();
		sumtask.stageid = js.getStageid();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		ToIntFunction<String[]> toint = (String str[]) -> Integer.parseInt(str[14]);
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(toint);
		js.getStage().tasks.add(new Sum());

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(sumtask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(sumtask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Integer> mapfiltermaxdata = (List<Integer>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		is.close();
		assertEquals(-63278, (int) mapfiltermaxdata.get(0));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapSD() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task sdtask = new Task();
		sdtask.jobid = js.getJobid();
		sdtask.stageid = js.getStageid();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		ToIntFunction<String[]> toint = (String str[]) -> Integer.parseInt(str[14]);
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(toint);
		js.getStage().tasks.add(new StandardDeviation());

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(sdtask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(sdtask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Double> mapfiltermaxdata = (List<Double>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		is.close();
		assertEquals(1, mapfiltermaxdata.size());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVCount() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task calcultecounttask = new Task();
		calcultecounttask.jobid = js.getJobid();
		calcultecounttask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(new CalculateCount());

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(calcultecounttask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(calcultecounttask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Long> mapfiltercountdata = (List<Long>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(45957l, (long) mapfiltercountdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecord() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(filtertask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(filtertask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<CSVRecord> filterdata = (List<CSVRecord>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(45957l, (long) filterdata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordSumaryStatistics() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task summarystaticstask = new Task();
		summarystaticstask.jobid = js.getJobid();
		summarystaticstask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new SummaryStatistics());

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(summarystaticstask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(summarystaticstask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<IntSummaryStatistics> mapfilterssdata = (List<IntSummaryStatistics>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(1, (long) mapfilterssdata.size());
		assertEquals(623, (long) mapfilterssdata.get(0).getMax());
		assertEquals(-89, (long) mapfilterssdata.get(0).getMin());
		assertEquals(-63278, (long) mapfilterssdata.get(0).getSum());
		assertEquals(45957l, mapfilterssdata.get(0).getCount());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordMax() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task maxtask = new Task();
		maxtask.jobid = js.getJobid();
		maxtask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Max());

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(maxtask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(maxtask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Integer> mapfilterssdata = (List<Integer>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(623, (long) mapfilterssdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordMin() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task mintask = new Task();
		mintask.jobid = js.getJobid();
		mintask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Min());

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(mintask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(mintask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Integer> mapfilterssdata = (List<Integer>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(-89, (long) mapfilterssdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordSum() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task sumtask = new Task();
		sumtask.jobid = js.getJobid();
		sumtask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Sum());

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(sumtask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(sumtask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Integer> mapfilterssdata = (List<Integer>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(-63278, (long) mapfilterssdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordStandardDeviation() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task sdtask = new Task();
		sdtask.jobid = js.getJobid();
		sdtask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new StandardDeviation());

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(sdtask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(sdtask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Double> mapfiltersddata = (List<Double>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(1, (long) mapfiltersddata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVCount() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(filtertask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsteim.getIntermediateDataFSFilePath(filtertask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		Task calcultecounttask = new Task();
		calcultecounttask.jobid = js.getJobid();
		calcultecounttask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(new CalculateCount());
		mdsteim.setTask(calcultecounttask);
		mdsteim.processBlockHDFSMap(inputtocount);
		is.close();
		
		path = mdsteim.getIntermediateDataFSFilePath(calcultecounttask);
		is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Long> csvreccount = (List<Long>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(45957l, (long) csvreccount.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVSummaryStatistics() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(filtertask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsteim.getIntermediateDataFSFilePath(filtertask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		Task summarystaticstask = new Task();
		summarystaticstask.jobid = js.getJobid();
		summarystaticstask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new SummaryStatistics());
		mdsteim.setTask(summarystaticstask);
		mdsteim.processBlockHDFSMap(inputtocount);
		is.close();
		
		path = mdsteim.getIntermediateDataFSFilePath(summarystaticstask);
		is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<IntSummaryStatistics> mapfilterssdata = (List<IntSummaryStatistics>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(1, (long) mapfilterssdata.size());
		assertEquals(623, (long) mapfilterssdata.get(0).getMax());
		assertEquals(-89, (long) mapfilterssdata.get(0).getMin());
		assertEquals(-63278, (long) mapfilterssdata.get(0).getSum());
		assertEquals(45957l, mapfilterssdata.get(0).getCount());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVMax() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(filtertask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsteim.getIntermediateDataFSFilePath(filtertask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		Task maxtask = new Task();
		maxtask.jobid = js.getJobid();
		maxtask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Max());
		mdsteim.setTask(maxtask);
		mdsteim.processBlockHDFSMap(inputtocount);
		is.close();
		
		path = mdsteim.getIntermediateDataFSFilePath(maxtask);
		is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Integer> mapfiltermaxdata = (List<Integer>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(623, (int) mapfiltermaxdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVMin() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(filtertask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsteim.getIntermediateDataFSFilePath(filtertask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		Task mintask = new Task();
		mintask.jobid = js.getJobid();
		mintask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Min());
		mdsteim.setTask(mintask);
		mdsteim.processBlockHDFSMap(inputtocount);
		is.close();
		
		path = mdsteim.getIntermediateDataFSFilePath(mintask);
		is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Integer> mapfiltermindata = (List<Integer>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(-89, (int) mapfiltermindata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVSum() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(filtertask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsteim.getIntermediateDataFSFilePath(filtertask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		Task sumtask = new Task();
		sumtask.jobid = js.getJobid();
		sumtask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Sum());
		mdsteim.setTask(sumtask);
		mdsteim.processBlockHDFSMap(inputtocount);
		is.close();
		
		path = mdsteim.getIntermediateDataFSFilePath(sumtask);
		is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Integer> mapfiltersumdata = (List<Integer>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(-63278, (int) mapfiltersumdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVSD() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(filtertask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsteim.getIntermediateDataFSFilePath(filtertask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		Task sdtask = new Task();
		sdtask.jobid = js.getJobid();
		sdtask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new StandardDeviation());
		mdsteim.setTask(sdtask);
		mdsteim.processBlockHDFSMap(inputtocount);
		is.close();
		
		path = mdsteim.getIntermediateDataFSFilePath(sdtask);
		is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Integer> mapfiltersddata = (List<Integer>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(1, (int) mapfiltersddata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessSample() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(filtertask);
		mdsteim.processSamplesBlocks(100, bls1.get(0), hdfs);
		String path = mdsteim.getIntermediateDataFSFilePath(filtertask);
		
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Long> csvreccount = (List<Long>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(100, (long) csvreccount.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessSampleCount() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task counttask = new Task();
		counttask.jobid = js.getJobid();
		counttask.stageid = js.getStageid();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(new CalculateCount());
		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(counttask);
		mdsteim.processSamplesBlocks(150, bls1.get(0), hdfs);
		String path = mdsteim.getIntermediateDataFSFilePath(counttask);
		
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Long> csvreccount = (List<Long>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(150l, (long) csvreccount.get(0));
		is.close();
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessStreamSample() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(filtertask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsteim.getIntermediateDataFSFilePath(filtertask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<InputStream> inputtocount = Arrays.asList(is);
		Task sample = new Task();
		sample.jobid = js.getJobid();
		sample.stageid = js.getStageid();
		Function samplefn = val -> val;
		js.getStage().tasks.clear();
		js.getStage().tasks.add(samplefn);
		mdsteim.setTask(sample);
		mdsteim.processSamplesObjects(150, inputtocount);
		is.close();
		
		path = mdsteim.getIntermediateDataFSFilePath(sample);
		is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Integer> mapfiltersddata = (List<Integer>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(150, (int) mapfiltersddata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamSampleCount() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(filtertask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdsteim.getIntermediateDataFSFilePath(filtertask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<InputStream> inputtocount = Arrays.asList(is);
		Task count = new Task();
		count.jobid = js.getJobid();
		count.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(new CalculateCount());
		mdsteim.setTask(count);
		mdsteim.processSamplesObjects(150, inputtocount);
		is.close();
		
		path = mdsteim.getIntermediateDataFSFilePath(count);
		is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Long> mapfiltersddata = (List<Long>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(150l, (long) mapfiltersddata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessJoin() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task reducebykeytask1 = new Task();
		reducebykeytask1.jobid = js.getJobid();
		reducebykeytask1.stageid = js.getStageid();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Integer>> pair = val -> new Tuple2<String, Integer>(
				(String) val[8], (Integer) Integer.parseInt(val[14]));
		ReduceByKeyFunction<Integer> redfunc = (input1, input2) -> input1 + input2;
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(reducebykeytask1);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		reducebykeytask2.jobid = js.getJobid();
		reducebykeytask2.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);
		mdsteim.setTask(reducebykeytask2);
		mdsteim.processBlockHDFSMap(bls2.get(0), hdfs);

		
		String path1 = mdsteim.getIntermediateDataFSFilePath(reducebykeytask1);
		InputStream is1 = new ByteArrayInputStream((byte[]) cache.get(path1));
		String path2 = mdsteim.getIntermediateDataFSFilePath(reducebykeytask2);
		InputStream is2 = new ByteArrayInputStream((byte[]) cache.get(path2));
		Task jointask = new Task();
		jointask.jobid = js.getJobid();
		jointask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		Consumer<String> dummy = val -> {
		};
		js.getStage().tasks.add(dummy);
		JoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdsteim.setTask(jointask);
		mdsteim.processJoin(is1, is2, jp, false, false);
		is1.close();
		is2.close();

		
		String path = mdsteim.getIntermediateDataFSFilePath(jointask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> mapfiltersddata = (List) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(1, (long) mapfiltersddata.size());
		Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>> tupleresult = mapfiltersddata.get(0);
		assertEquals(tupleresult.v1.v1, tupleresult.v2.v1);
		assertEquals(tupleresult.v1.v2, tupleresult.v2.v2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessLeftOuterJoin() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task reducebykeytask1 = new Task();
		reducebykeytask1.jobid = js.getJobid();
		reducebykeytask1.stageid = js.getStageid();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Integer>> pair = val -> new Tuple2<String, Integer>(
				(String) val[8], (Integer) Integer.parseInt(val[14]));
		ReduceByKeyFunction<Integer> redfunc = (input1, input2) -> input1 + input2;
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(reducebykeytask1);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		reducebykeytask2.jobid = js.getJobid();
		reducebykeytask2.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);
		mdsteim.setTask(reducebykeytask2);
		mdsteim.processBlockHDFSMap(bls2.get(0), hdfs);

		
		String path1 = mdsteim.getIntermediateDataFSFilePath(reducebykeytask1);
		InputStream is1 = new ByteArrayInputStream((byte[]) cache.get(path1));
		String path2 = mdsteim.getIntermediateDataFSFilePath(reducebykeytask2);
		InputStream is2 = new ByteArrayInputStream((byte[]) cache.get(path2));
		Task jointask = new Task();
		jointask.jobid = js.getJobid();
		jointask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		Consumer<String> dummy = val -> {
		};
		js.getStage().tasks.add(dummy);

		LeftOuterJoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdsteim.setTask(jointask);
		mdsteim.processLeftOuterJoin(is1, is2, jp, false, false);
		is1.close();
		is2.close();

		
		String path = mdsteim.getIntermediateDataFSFilePath(jointask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> mapfiltersddata = (List) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
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
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task reducebykeytask1 = new Task();
		reducebykeytask1.jobid = js.getJobid();
		reducebykeytask1.stageid = js.getStageid();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Integer>> pair = val -> new Tuple2<String, Integer>(
				(String) val[8], (Integer) Integer.parseInt(val[14]));
		ReduceByKeyFunction<Integer> redfunc = (input1, input2) -> input1 + input2;
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(reducebykeytask1);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		reducebykeytask2.jobid = js.getJobid();
		reducebykeytask2.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);
		mdsteim.setTask(reducebykeytask2);
		mdsteim.processBlockHDFSMap(bls2.get(0), hdfs);

		
		String path1 = mdsteim.getIntermediateDataFSFilePath(reducebykeytask1);
		InputStream is1 = new ByteArrayInputStream((byte[]) cache.get(path1));
		String path2 = mdsteim.getIntermediateDataFSFilePath(reducebykeytask2);
		InputStream is2 = new ByteArrayInputStream((byte[]) cache.get(path2));
		Task jointask = new Task();
		jointask.jobid = js.getJobid();
		jointask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		Consumer<String> dummy = val -> {
		};
		js.getStage().tasks.add(dummy);

		RightOuterJoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdsteim.setTask(jointask);
		mdsteim.processRightOuterJoin(is2, is1, jp, false, false);
		is1.close();
		is2.close();

		
		String path = mdsteim.getIntermediateDataFSFilePath(jointask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> mapfiltersddata = (List) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
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
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task mappairtask1 = new Task();
		mappairtask1.jobid = js.getJobid();
		mappairtask1.stageid = js.getStageid();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Integer>> pair = val -> new Tuple2<String, Integer>(
				(String) val[8], (Integer) Integer.parseInt(val[14]));
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(mappairtask1);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);

		
		String path1 = mdsteim.getIntermediateDataFSFilePath(mappairtask1);
		InputStream is1 = new ByteArrayInputStream((byte[]) cache.get(path1));
		Task gbktask = new Task();
		gbktask.jobid = js.getJobid();
		gbktask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(gbktask);
		Consumer<String> dummy = val -> {
		};
		gbktask.input = new Object[]{is1};
		mdsteim.setTask(gbktask);
		mdsteim.processGroupByKeyTuple2();
		is1.close();

		
		String path = mdsteim.getIntermediateDataFSFilePath(gbktask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Tuple2<String, List<Integer>>> mapfiltersddata = (List) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(1, (long) mapfiltersddata.size());
		Tuple2<String, List<Integer>> tupleresult = mapfiltersddata.get(0);
		assertEquals(45957l, (int) tupleresult.v2.size());
		assertEquals("AQ", tupleresult.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessFoldLeft() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task mappairtask1 = new Task();
		mappairtask1.jobid = js.getJobid();
		mappairtask1.stageid = js.getStageid();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Long>> pair = val -> new Tuple2<String, Long>((String) val[8],
				(Long) Long.parseLong(val[14]));
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(mappairtask1);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);

		
		String path1 = mdsteim.getIntermediateDataFSFilePath(mappairtask1);
		InputStream is1 = new ByteArrayInputStream((byte[]) cache.get(path1));
		Task fbktask = new Task();
		fbktask.jobid = js.getJobid();
		fbktask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		ReduceByKeyFunction<Long> redfunc = (a, b) -> a + b;
		FoldByKey fbk = new FoldByKey(0l, redfunc, true);
		js.getStage().tasks.add(fbk);
		fbktask.input = new Object[]{is1};
		mdsteim.setTask(fbktask);
		mdsteim.processFoldByKeyTuple2();
		is1.close();

		
		String path = mdsteim.getIntermediateDataFSFilePath(fbktask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Tuple2<String, Long>> mapfiltersddata = (List) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();

		Tuple2<String, Long> tupleresult = mapfiltersddata.get(0);
		assertEquals(-63278l, (long) tupleresult.v2);
		assertEquals("AQ", tupleresult.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessFoldRight() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task mappairtask1 = new Task();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Long>> pair = val -> new Tuple2<String, Long>((String) val[8],
				(Long) Long.parseLong(val[14]));
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(mappairtask1);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);

		
		String path1 = mdsteim.getIntermediateDataFSFilePath(mappairtask1);
		InputStream is1 = new ByteArrayInputStream((byte[]) cache.get(path1));
		Task fbktask = new Task();
		fbktask.jobid = js.getJobid();
		fbktask.stageid = js.getStageid();
		ReduceByKeyFunction<Long> redfunc = (a, b) -> a + b;
		FoldByKey fbk = new FoldByKey(0l, redfunc, false);
		js.getStage().tasks.clear();
		js.getStage().tasks.add(fbk);

		fbktask.input = new Object[]{is1};
		mdsteim.setTask(fbktask);
		mdsteim.processFoldByKeyTuple2();
		is1.close();

		
		String path = mdsteim.getIntermediateDataFSFilePath(fbktask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Tuple2<String, Long>> mapfiltersddata = (List) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();

		Tuple2<String, Long> tupleresult = mapfiltersddata.get(0);
		assertEquals(-63278l, (long) tupleresult.v2);
		assertEquals("AQ", tupleresult.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessCountByKey() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task mappairtask1 = new Task();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Long>> pair = val -> new Tuple2<String, Long>((String) val[8],
				(Long) Long.parseLong(val[14]));
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(mappairtask1);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);

		
		String path1 = mdsteim.getIntermediateDataFSFilePath(mappairtask1);
		InputStream is1 = new ByteArrayInputStream((byte[]) cache.get(path1));
		Task cbktask = new Task();
		cbktask.jobid = js.getJobid();
		cbktask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(new CountByKeyFunction());
		cbktask.input = new Object[]{is1};
		mdsteim.setTask(cbktask);
		mdsteim.processCountByKeyTuple2();
		is1.close();

		
		String path = mdsteim.getIntermediateDataFSFilePath(cbktask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Tuple2<String, Long>> mapfiltersddata = (List) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();

		Tuple2<String, Long> tupleresult = mapfiltersddata.get(0);
		assertEquals(45957l, (long) tupleresult.v2);
		assertEquals("AQ", tupleresult.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessCountByValue() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task mappairtask1 = new Task();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Long>> pair = val -> new Tuple2<String, Long>((String) val[8],
				(Long) Long.parseLong(val[14]));
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(mappairtask1);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);

		
		String path1 = mdsteim.getIntermediateDataFSFilePath(mappairtask1);
		InputStream is1 = new ByteArrayInputStream((byte[]) cache.get(path1));
		Task cbktask = new Task();
		cbktask.jobid = js.getJobid();
		cbktask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(new CountByKeyFunction());
		cbktask.input = new Object[]{is1};
		mdsteim.setTask(cbktask);
		mdsteim.processCountByValueTuple2();
		is1.close();

		
		String path = mdsteim.getIntermediateDataFSFilePath(cbktask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Tuple2<Tuple2<String, Long>, Long>> mapfiltersddata = (List) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
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
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task reducebykeytask1 = new Task();
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Integer>> pair = val -> new Tuple2<String, Integer>(
				(String) val[8], (Integer) Integer.parseInt(val[14]));
		ReduceByKeyFunction<Integer> redfunc = (input1, input2) -> input1 + input2;
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(reducebykeytask1);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);
		mdsteim.setTask(reducebykeytask2);
		mdsteim.processBlockHDFSMap(bls2.get(0), hdfs);

		
		String path1 = mdsteim.getIntermediateDataFSFilePath(reducebykeytask1);
		InputStream is1 = new ByteArrayInputStream((byte[]) cache.get(path1));
		String path2 = mdsteim.getIntermediateDataFSFilePath(reducebykeytask2);
		InputStream is2 = new ByteArrayInputStream((byte[]) cache.get(path2));
		reducebykeytask2.input = new Object[]{is1, is2};
		Task coalescetask = new Task();
		coalescetask.jobid = js.getJobid();
		coalescetask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		Coalesce<Integer> coalesce = new Coalesce();
		coalesce.setCoalescepartition(1);
		coalesce.setCoalescefunction((a, b) -> a + b);
		coalescetask.input = new Object[]{is1, is2};
		js.getStage().tasks.add(coalesce);
		RightOuterJoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdsteim.setTask(coalescetask);
		mdsteim.processCoalesce();
		is1.close();
		is2.close();

		
		String path = mdsteim.getIntermediateDataFSFilePath(coalescetask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<Tuple2<String, Integer>> mapfiltersddata = (List) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
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
		js.setStage(new Stage());
		js.setJobid(MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Json json = new Json();
		Task filtertask = new Task();
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();
		PredicateSerializable<JSONObject> filter = jsonobj -> jsonobj != null
				&& jsonobj.get("type").equals("CreateEvent");
		js.getStage().tasks.add(json);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorInMemoryDisk mdsteim = new StreamPipelineTaskExecutorInMemoryDisk(js, resultstream,
				MDCCache.get());
		mdsteim.setHdfs(hdfs);
		mdsteim.setExecutor(es);
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
		mdsteim.setTask(filtertask);
		mdsteim.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdsteim.getIntermediateDataFSFilePath(filtertask);
		InputStream is = new ByteArrayInputStream((byte[]) cache.get(path));
		List<JSONObject> jsonfilterdata = (List<JSONObject>) new FSTObjectInput(is, Utils.getConfigForSerialization()).readObject();
		assertEquals(11l, (long) jsonfilterdata.size());
		is.close();
	}

	// JSON Test cases End
	
}
