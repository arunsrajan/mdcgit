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
import java.util.Set;
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
import com.github.mdc.common.MDCProperties;
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

public class StreamPipelineTaskExecutorTest extends StreamPipelineTestCommon {

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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(task);
		mdstde.processBlockHDFSIntersection(bls.get(0), bls.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(task);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<String> intersectiondata = (List<String>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(46361, intersectiondata.size());
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(task);
		mdstde.processBlockHDFSIntersection(bls1.get(0), bls2.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(task);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<String> intersectiondata = (List<String>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(20, intersectiondata.size());
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(task);
		mdstde.processBlockHDFSIntersection(bls1.get(0), bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(task);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		Set<InputStream> istreams = new LinkedHashSet<>(Arrays.asList(fsdis));
		task = new Task();
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		function = new IntersectionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdstde.setTask(task);
		mdstde.processBlockHDFSIntersection(istreams, Arrays.asList(bls2.get(0)), hdfs);
		fsdis.close();
		mdstde.setTask(task);
		path = mdstde.getIntermediateDataFSFilePath(task);
		fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<String> intersectiondata = (List<String>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(20, intersectiondata.size());
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(task1);
		mdstde.processBlockHDFSIntersection(bls1.get(0), bls1.get(0), hdfs);
		Task task2 = new Task();
		task2.jobid = js.getJobid();
		task2.stageid = js.getStageid();
		function = new IntersectionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdstde.setTask(task2);
		mdstde.processBlockHDFSIntersection(bls2.get(0), bls2.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(task1);
		InputStream fsdis1 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<InputStream> istreams1 = Arrays.asList(fsdis1);
		path = mdstde.getIntermediateDataFSFilePath(task2);
		InputStream fsdis2 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<InputStream> istreams2 = Arrays.asList(fsdis2);

		Task taskinter = new Task();
		taskinter.jobid = js.getJobid();
		taskinter.stageid = js.getStageid();
		function = new IntersectionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdstde.setTask(taskinter);
		mdstde.processBlockHDFSIntersection(istreams1, istreams2);
		fsdis1.close();
		fsdis2.close();

		path = mdstde.getIntermediateDataFSFilePath(taskinter);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<String> intersectiondata = (List<String>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(20, intersectiondata.size());
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(task);
		mdstde.processBlockHDFSUnion(bls.get(0), bls.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(task);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<String> uniondata = (List<String>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(46361, uniondata.size());
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(task);
		mdstde.processBlockHDFSUnion(bls1.get(0), bls2.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(task);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<String> uniondata = (List<String>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(60, uniondata.size());
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(task);
		mdstde.processBlockHDFSUnion(bls1.get(0), bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(task);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		Set<InputStream> istreams = new LinkedHashSet<>(Arrays.asList(fsdis));
		task = new Task();
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		function = new UnionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdstde.setTask(task);
		mdstde.processBlockHDFSUnion(istreams, Arrays.asList(bls2.get(0)), hdfs);
		fsdis.close();
		path = mdstde.getIntermediateDataFSFilePath(task);
		fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<String> uniondata = (List<String>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(60, uniondata.size());
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(task1);
		mdstde.processBlockHDFSUnion(bls1.get(0), bls1.get(0), hdfs);
		Task task2 = new Task();
		task2.jobid = js.getJobid();
		task2.stageid = js.getStageid();
		function = new UnionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdstde.setTask(task2);
		mdstde.processBlockHDFSUnion(bls2.get(0), bls2.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(task1);
		InputStream fsdis1 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<InputStream> istreams1 = Arrays.asList(fsdis1);

		path = mdstde.getIntermediateDataFSFilePath(task2);
		InputStream fsdis2 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<InputStream> istreams2 = Arrays.asList(fsdis2);

		Task taskunion = new Task();
		taskunion.jobid = js.getJobid();
		taskunion.stageid = js.getStageid();
		function = new UnionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdstde.setTask(taskunion);
		mdstde.processBlockHDFSUnion(istreams1, istreams2);
		fsdis1.close();
		fsdis2.close();

		path = mdstde.getIntermediateDataFSFilePath(taskunion);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<String> uniondata = (List<String>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(60, uniondata.size());
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(filtertask);

		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<String[]> mapfilterdata = (List<String[]>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(45957, mapfilterdata.size());
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(calculatecounttask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(calculatecounttask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Long> mapfiltercountdata = (List<Long>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(45957l, (long) mapfiltercountdata.get(0));
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(sstask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(sstask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<IntSummaryStatistics> mapfilterssdata = (List<IntSummaryStatistics>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(maxtask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(maxtask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Integer> mapfiltermaxdata = (List<Integer>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(mintask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(mintask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Integer> mapfiltermaxdata = (List<Integer>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(sumtask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(sumtask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Integer> mapfiltermaxdata = (List<Integer>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(sdtask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(sdtask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Double> mapfiltermaxdata = (List<Double>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(calcultecounttask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(calcultecounttask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Long> mapfiltercountdata = (List<Long>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(45957l, (long) mapfiltercountdata.get(0));
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(filtertask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<CSVRecord> filterdata = (List<CSVRecord>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(45957l, (long) filterdata.size());
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(summarystaticstask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(summarystaticstask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<IntSummaryStatistics> mapfilterssdata = (List<IntSummaryStatistics>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(maxtask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(maxtask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Integer> mapfilterssdata = (List<Integer>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(623, (long) mapfilterssdata.get(0));
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(mintask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(mintask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Integer> mapfilterssdata = (List<Integer>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(-89, (long) mapfilterssdata.get(0));
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(sumtask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(sumtask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Integer> mapfilterssdata = (List<Integer>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(-63278, (long) mapfilterssdata.get(0));
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(sdtask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(sdtask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Double> mapfiltersddata = (List<Double>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(1, (long) mapfiltersddata.size());
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(filtertask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdstde.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(fsdis));
		Task calcultecounttask = new Task();
		calcultecounttask.jobid = js.getJobid();
		calcultecounttask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(new CalculateCount());
		mdstde.setTask(calcultecounttask);
		mdstde.processBlockHDFSMap(inputtocount);
		fsdis.close();
		
		path = mdstde.getIntermediateDataFSFilePath(calcultecounttask);
		fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Long> csvreccount = (List<Long>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(45957l, (long) csvreccount.get(0));
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(filtertask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdstde.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(fsdis));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		Task summarystaticstask = new Task();
		summarystaticstask.jobid = js.getJobid();
		summarystaticstask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new SummaryStatistics());
		mdstde.setTask(summarystaticstask);
		mdstde.processBlockHDFSMap(inputtocount);
		fsdis.close();
		
		path = mdstde.getIntermediateDataFSFilePath(summarystaticstask);
		fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<IntSummaryStatistics> mapfilterssdata = (List<IntSummaryStatistics>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(filtertask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdstde.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(fsdis));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		Task maxtask = new Task();
		maxtask.jobid = js.getJobid();
		maxtask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Max());
		mdstde.setTask(maxtask);
		mdstde.processBlockHDFSMap(inputtocount);
		fsdis.close();
		
		path = mdstde.getIntermediateDataFSFilePath(maxtask);
		fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Integer> mapfiltermaxdata = (List<Integer>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(623, (int) mapfiltermaxdata.get(0));
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(filtertask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdstde.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(fsdis));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		Task mintask = new Task();
		mintask.jobid = js.getJobid();
		mintask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Min());
		mdstde.setTask(mintask);
		mdstde.processBlockHDFSMap(inputtocount);
		fsdis.close();
		
		path = mdstde.getIntermediateDataFSFilePath(mintask);
		fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Integer> mapfiltermindata = (List<Integer>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(-89, (int) mapfiltermindata.get(0));
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(filtertask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdstde.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(fsdis));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		Task sumtask = new Task();
		sumtask.jobid = js.getJobid();
		sumtask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Sum());
		mdstde.setTask(sumtask);
		mdstde.processBlockHDFSMap(inputtocount);
		fsdis.close();
		
		path = mdstde.getIntermediateDataFSFilePath(sumtask);
		fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Integer> mapfiltersumdata = (List<Integer>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(-63278, (int) mapfiltersumdata.get(0));
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(filtertask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdstde.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(fsdis));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		Task sdtask = new Task();
		sdtask.jobid = js.getJobid();
		sdtask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new StandardDeviation());
		mdstde.setTask(sdtask);
		mdstde.processBlockHDFSMap(inputtocount);
		fsdis.close();
		
		path = mdstde.getIntermediateDataFSFilePath(sdtask);
		fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Integer> mapfiltersddata = (List<Integer>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(1, (int) mapfiltersddata.size());
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(filtertask);
		mdstde.processSamplesBlocks(100, bls1.get(0), hdfs);
		String path = mdstde.getIntermediateDataFSFilePath(filtertask);
		
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Long> csvreccount = (List<Long>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(100, (long) csvreccount.size());
		fsdis.close();
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
		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(counttask);
		mdstde.processSamplesBlocks(150, bls1.get(0), hdfs);
		String path = mdstde.getIntermediateDataFSFilePath(counttask);
		
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Long> csvreccount = (List<Long>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(150l, (long) csvreccount.get(0));
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(filtertask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdstde.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<InputStream> inputtocount = Arrays.asList(fsdis);
		Task sample = new Task();
		sample.jobid = js.getJobid();
		sample.stageid = js.getStageid();
		Function samplefn = val -> val;
		js.getStage().tasks.clear();
		js.getStage().tasks.add(samplefn);
		mdstde.setTask(sample);
		mdstde.processSamplesObjects(150, inputtocount);
		fsdis.close();
		
		path = mdstde.getIntermediateDataFSFilePath(sample);
		fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Integer> mapfiltersddata = (List<Integer>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(150, (int) mapfiltersddata.size());
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(filtertask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		String path = mdstde.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<InputStream> inputtocount = Arrays.asList(fsdis);
		Task count = new Task();
		count.jobid = js.getJobid();
		count.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(new CalculateCount());
		mdstde.setTask(count);
		mdstde.processSamplesObjects(150, inputtocount);
		fsdis.close();
		
		path = mdstde.getIntermediateDataFSFilePath(count);
		fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Long> mapfiltersddata = (List<Long>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(150l, (long) mapfiltersddata.get(0));
		fsdis.close();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(reducebykeytask1);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		reducebykeytask2.jobid = js.getJobid();
		reducebykeytask2.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);
		mdstde.setTask(reducebykeytask2);
		mdstde.processBlockHDFSMap(bls2.get(0), hdfs);

		
		String path1 = mdstde.getIntermediateDataFSFilePath(reducebykeytask1);
		InputStream fsdis1 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 ));
		String path2 = mdstde.getIntermediateDataFSFilePath(reducebykeytask2);
		InputStream fsdis2 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path2 ));
		Task jointask = new Task();
		jointask.jobid = js.getJobid();
		jointask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		Consumer<String> dummy = val -> {
		};
		js.getStage().tasks.add(dummy);
		JoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdstde.setTask(jointask);
		mdstde.processJoin(fsdis1, fsdis2, jp, false, false);
		fsdis1.close();
		fsdis2.close();

		
		String path = mdstde.getIntermediateDataFSFilePath(jointask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> mapfiltersddata = (List) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(reducebykeytask1);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		reducebykeytask2.jobid = js.getJobid();
		reducebykeytask2.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);
		mdstde.setTask(reducebykeytask2);
		mdstde.processBlockHDFSMap(bls2.get(0), hdfs);

		
		String path1 = mdstde.getIntermediateDataFSFilePath(reducebykeytask1);
		InputStream fsdis1 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 ));
		String path2 = mdstde.getIntermediateDataFSFilePath(reducebykeytask2);
		InputStream fsdis2 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path2 ));
		Task jointask = new Task();
		jointask.jobid = js.getJobid();
		jointask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		Consumer<String> dummy = val -> {
		};
		js.getStage().tasks.add(dummy);

		LeftOuterJoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdstde.setTask(jointask);
		mdstde.processLeftOuterJoin(fsdis1, fsdis2, jp, false, false);
		fsdis1.close();
		fsdis2.close();

		
		String path = mdstde.getIntermediateDataFSFilePath(jointask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> mapfiltersddata = (List) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(reducebykeytask1);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		reducebykeytask2.jobid = js.getJobid();
		reducebykeytask2.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);
		mdstde.setTask(reducebykeytask2);
		mdstde.processBlockHDFSMap(bls2.get(0), hdfs);

		
		String path1 = mdstde.getIntermediateDataFSFilePath(reducebykeytask1);
		InputStream fsdis1 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 ));
		String path2 = mdstde.getIntermediateDataFSFilePath(reducebykeytask2);
		InputStream fsdis2 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path2 ));
		Task jointask = new Task();
		jointask.jobid = js.getJobid();
		jointask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		Consumer<String> dummy = val -> {
		};
		js.getStage().tasks.add(dummy);

		RightOuterJoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdstde.setTask(jointask);
		mdstde.processRightOuterJoin(fsdis2, fsdis1, jp, false, false);
		fsdis1.close();
		fsdis2.close();

		
		String path = mdstde.getIntermediateDataFSFilePath(jointask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> mapfiltersddata = (List) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(mappairtask1);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);

		
		String path1 = mdstde.getIntermediateDataFSFilePath(mappairtask1);
		InputStream fsdis1 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 ));
		Task gbktask = new Task();
		gbktask.jobid = js.getJobid();
		gbktask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(gbktask);
		Consumer<String> dummy = val -> {
		};
		gbktask.input = new Object[]{fsdis1};
		mdstde.setTask(gbktask);
		mdstde.processGroupByKeyTuple2();
		fsdis1.close();

		
		String path = mdstde.getIntermediateDataFSFilePath(gbktask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Tuple2<String, List<Integer>>> mapfiltersddata = (List) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(mappairtask1);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);

		
		String path1 = mdstde.getIntermediateDataFSFilePath(mappairtask1);
		InputStream fsdis1 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 ));
		Task fbktask = new Task();
		fbktask.jobid = js.getJobid();
		fbktask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		ReduceByKeyFunction<Long> redfunc = (a, b) -> a + b;
		FoldByKey fbk = new FoldByKey(0l, redfunc, true);
		js.getStage().tasks.add(fbk);
		fbktask.input = new Object[]{fsdis1};
		mdstde.setTask(fbktask);
		mdstde.processFoldByKeyTuple2();
		fsdis1.close();

		
		String path = mdstde.getIntermediateDataFSFilePath(fbktask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Tuple2<String, Long>> mapfiltersddata = (List) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();

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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(mappairtask1);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);

		
		String path1 = mdstde.getIntermediateDataFSFilePath(mappairtask1);
		InputStream fsdis1 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 ));
		Task fbktask = new Task();
		fbktask.jobid = js.getJobid();
		fbktask.stageid = js.getStageid();
		ReduceByKeyFunction<Long> redfunc = (a, b) -> a + b;
		FoldByKey fbk = new FoldByKey(0l, redfunc, false);
		js.getStage().tasks.clear();
		js.getStage().tasks.add(fbk);

		fbktask.input = new Object[]{fsdis1};
		mdstde.setTask(fbktask);
		mdstde.processFoldByKeyTuple2();
		fsdis1.close();

		
		String path = mdstde.getIntermediateDataFSFilePath(fbktask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Tuple2<String, Long>> mapfiltersddata = (List) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();

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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(mappairtask1);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);

		
		String path1 = mdstde.getIntermediateDataFSFilePath(mappairtask1);
		InputStream fsdis1 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 ));
		Task cbktask = new Task();
		cbktask.jobid = js.getJobid();
		cbktask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(new CountByKeyFunction());
		cbktask.input = new Object[]{fsdis1};
		mdstde.setTask(cbktask);
		mdstde.processCountByKeyTuple2();
		fsdis1.close();

		
		String path = mdstde.getIntermediateDataFSFilePath(cbktask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Tuple2<String, Long>> mapfiltersddata = (List) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();

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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(mappairtask1);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);

		
		String path1 = mdstde.getIntermediateDataFSFilePath(mappairtask1);
		InputStream fsdis1 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 ));
		Task cbktask = new Task();
		cbktask.jobid = js.getJobid();
		cbktask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(new CountByKeyFunction());
		cbktask.input = new Object[]{fsdis1};
		mdstde.setTask(cbktask);
		mdstde.processCountByValueTuple2();
		fsdis1.close();

		
		String path = mdstde.getIntermediateDataFSFilePath(cbktask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Tuple2<Tuple2<String, Long>, Long>> mapfiltersddata = (List) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(reducebykeytask1);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);
		mdstde.setTask(reducebykeytask2);
		mdstde.processBlockHDFSMap(bls2.get(0), hdfs);

		
		String path1 = mdstde.getIntermediateDataFSFilePath(reducebykeytask1);
		InputStream fsdis1 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path1 ));
		String path2 = mdstde.getIntermediateDataFSFilePath(reducebykeytask2);
		InputStream fsdis2 = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path2 ));
		reducebykeytask2.input = new Object[]{fsdis1, fsdis2};
		Task coalescetask = new Task();
		coalescetask.jobid = js.getJobid();
		coalescetask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		Coalesce<Integer> coalesce = new Coalesce();
		coalesce.setCoalescepartition(1);
		coalesce.setCoalescefunction((a, b) -> a + b);
		coalescetask.input = new Object[]{fsdis1, fsdis2};
		js.getStage().tasks.add(coalesce);
		mdstde.setTask(coalescetask);
		mdstde.processCoalesce();
		fsdis1.close();
		fsdis2.close();

		
		String path = mdstde.getIntermediateDataFSFilePath(coalescetask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<Tuple2<String, Integer>> mapfiltersddata = (List) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
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

		StreamPipelineTaskExecutor mdstde = new StreamPipelineTaskExecutor(js, MDCCache.get());
		mdstde.setHdfs(hdfs);
		mdstde.setExecutor(es);
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
		mdstde.setTask(filtertask);
		mdstde.processBlockHDFSMap(bls1.get(0), hdfs);
		
		String path = mdstde.getIntermediateDataFSFilePath(filtertask);
		InputStream fsdis = new BufferedInputStream(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.TMPDIR) + path ));
		List<JSONObject> jsonfilterdata = (List<JSONObject>) new FSTObjectInput(fsdis, Utils.getConfigForSerialization()).readObject();
		assertEquals(11l, (long) jsonfilterdata.size());
		fsdis.close();
	}
	// JSON Test cases End

}
