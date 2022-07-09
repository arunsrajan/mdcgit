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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.jooq.lambda.tuple.Tuple2;
import org.json.simple.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.HDFSBlockUtils;
import com.github.mdc.common.HdfsBlockReader;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.Stage;
import com.github.mdc.common.Task;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.CsvOptions;
import com.github.mdc.stream.Json;
import com.github.mdc.stream.StreamPipelineTestCommon;
import com.github.mdc.stream.functions.CalculateCount;
import com.github.mdc.stream.functions.Coalesce;
import com.github.mdc.stream.functions.CountByKeyFunction;
import com.github.mdc.stream.functions.FoldByKey;
import com.github.mdc.stream.functions.IntersectionFunction;
import com.github.mdc.stream.functions.JoinPredicate;
import com.github.mdc.stream.functions.LeftOuterJoinPredicate;
import com.github.mdc.stream.functions.MapFunction;
import com.github.mdc.stream.functions.MapToPairFunction;
import com.github.mdc.stream.functions.Max;
import com.github.mdc.stream.functions.Min;
import com.github.mdc.stream.functions.PredicateSerializable;
import com.github.mdc.stream.functions.ReduceByKeyFunction;
import com.github.mdc.stream.functions.RightOuterJoinPredicate;
import com.github.mdc.stream.functions.StandardDeviation;
import com.github.mdc.stream.functions.Sum;
import com.github.mdc.stream.functions.SummaryStatistics;
import com.github.mdc.stream.functions.UnionFunction;
import com.github.mdc.stream.utils.FileBlocksPartitionerHDFS;
import com.github.mdc.stream.utils.MDCIgniteServer;

public class StreamPipelineTaskExecutorIgniteTest extends StreamPipelineTestCommon {
	ConcurrentMap<String, ByteArrayOutputStream> resultstream = new ConcurrentHashMap<>();
	static Ignite igniteserver;
	static IgniteCache<Object, byte[]> ignitecache;

	@BeforeClass
	public static void launchNodes() throws Exception {
		Utils.loadLog4JSystemPropertiesClassPath("mdctest.properties");

		// Starting the node
		igniteserver = MDCIgniteServer.instance();
		ignitecache = igniteserver.getOrCreateCache(MDCConstants.MDCCACHE);
	}

	// CSV Test Cases Start
	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSIntersection() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		Object function = new IntersectionFunction();
		js.stage.tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, task);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls.get(0));
		mdsti.processBlockHDFSIntersection(bls.get(0), bls.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();

		InputStream is = mdsti.getIntermediateInputStreamFS(task.jobid + task.stageid + task.taskid);
		List<String> intersectiondata = (List<String>) kryo.readClassAndObject(new Input(is));
		assertEquals(46361, intersectiondata.size());
		is.close();
	}

	public void sendDataBlockToIgniteServer(BlocksLocation bsl) {
		try (var baos = new ByteArrayOutputStream(); var lzfos = new SnappyOutputStream(baos);) {
			var databytes = HdfsBlockReader.getBlockDataMR(bsl, hdfs);
			lzfos.write(databytes);
			lzfos.flush();
			ignitecache.putIfAbsent(bsl, baos.toByteArray());
		} catch (Exception e) {

		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSIntersectionDiff() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		Object function = new IntersectionFunction();
		js.stage.tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, task);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, true,
				128 * MDCConstants.MB);
		fbp.getDnXref(bls2, false);
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.processBlockHDFSIntersection(bls1.get(0), bls2.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();

		InputStream is = mdsti.getIntermediateInputStreamFS(task.jobid + task.stageid + task.taskid);
		List<String> intersectiondata = (List<String>) kryo.readClassAndObject(new Input(is));
		assertEquals(20, intersectiondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSIntersectionDiff() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		Object function = new IntersectionFunction();
		js.stage.tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, task);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, true,
				128 * MDCConstants.MB);
		fbp.getDnXref(bls2, false);
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.processBlockHDFSIntersection(bls1.get(0), bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();

		InputStream is = mdsti.getIntermediateInputStreamFS(task.jobid + task.stageid + task.taskid);
		Set<InputStream> istreams = new LinkedHashSet<>(Arrays.asList(is));
		task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		function = new IntersectionFunction();
		js.stage.tasks.clear();
		js.stage.tasks.add(function);
		mdsti = new StreamPipelineTaskExecutorIgnite(js, task);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.processBlockHDFSIntersection(istreams, Arrays.asList(bls2.get(0)), hdfs);
		is.close();
		is = mdsti.getIntermediateInputStreamFS(task.jobid + task.stageid + task.taskid);
		List<String> intersectiondata = (List<String>) kryo.readClassAndObject(new Input(is));
		assertEquals(20, intersectiondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamsBlockHDFSIntersectionDiff() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		Task task1 = new Task();
		task1.jobid = js.jobid;
		task1.stageid = js.stageid;
		Object function = new IntersectionFunction();
		js.stage.tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, task1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.processBlockHDFSIntersection(bls1.get(0), bls1.get(0), hdfs);
		Task task2 = new Task();
		task2.jobid = js.jobid;
		task2.stageid = js.stageid;
		function = new IntersectionFunction();
		js.stage.tasks.clear();
		js.stage.tasks.add(function);
		InputStream is1 = mdsti.getIntermediateInputStreamFS(task1.jobid + task1.stageid + task1.taskid);
		List<InputStream> istreams1 = Arrays.asList(is1);
		mdsti.setTask(task2);
		mdsti.processBlockHDFSIntersection(bls2.get(0), bls2.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		mdsti.setTask(task2);
		InputStream is2 = mdsti.getIntermediateInputStreamFS(task2.jobid + task2.stageid + task2.taskid);
		List<InputStream> istreams2 = Arrays.asList(is2);

		Task taskinter = new Task();
		taskinter.jobid = js.jobid;
		taskinter.stageid = js.stageid;
		mdsti.setTask(taskinter);
		function = new IntersectionFunction();
		js.stage.tasks.clear();
		js.stage.tasks.add(function);
		mdsti.setTask(taskinter);
		mdsti.processBlockHDFSIntersection(istreams1, istreams2);
		is1.close();
		is2.close();

		InputStream is = mdsti.getIntermediateInputStreamFS(taskinter.jobid + taskinter.stageid + taskinter.taskid);
		List<String> intersectiondata = (List<String>) kryo.readClassAndObject(new Input(is));
		assertEquals(20, intersectiondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSUnion() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		Object function = new UnionFunction();
		js.stage.tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, task);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls.get(0));
		mdsti.processBlockHDFSUnion(bls.get(0), bls.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();

		InputStream is = mdsti.getIntermediateInputStreamFS(task.jobid + task.stageid + task.taskid);
		List<String> uniondata = (List<String>) kryo.readClassAndObject(new Input(is));
		assertEquals(46361, uniondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSUnionDiff() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		Object function = new UnionFunction();
		js.stage.tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, task);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.processBlockHDFSUnion(bls1.get(0), bls2.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();

		InputStream is = mdsti.getIntermediateInputStreamFS(task.jobid + task.stageid + task.taskid);
		List<String> uniondata = (List<String>) kryo.readClassAndObject(new Input(is));
		assertEquals(60, uniondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSUnionDiff() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		Task task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		Object function = new UnionFunction();
		js.stage.tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, task);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.processBlockHDFSUnion(bls1.get(0), bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();

		InputStream is = mdsti.getIntermediateInputStreamFS(task.jobid + task.stageid + task.taskid);
		Set<InputStream> istreams = new LinkedHashSet<>(Arrays.asList(is));
		task = new Task();
		task.jobid = js.jobid;
		task.stageid = js.stageid;
		function = new UnionFunction();
		js.stage.tasks.clear();
		js.stage.tasks.add(function);
		mdsti.setTask(task);
		mdsti.processBlockHDFSUnion(istreams, Arrays.asList(bls2.get(0)), hdfs);
		is.close();

		is = mdsti.getIntermediateInputStreamFS(task.jobid + task.stageid + task.taskid);
		List<String> uniondata = (List<String>) kryo.readClassAndObject(new Input(is));
		assertEquals(60, uniondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamsBlockHDFSUnionDiff() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		Task task1 = new Task();
		task1.jobid = js.jobid;
		task1.stageid = js.stageid;
		Object function = new UnionFunction();
		js.stage.tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, task1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));

		mdsti.processBlockHDFSUnion(bls1.get(0), bls1.get(0), hdfs);
		Task task2 = new Task();
		task2.jobid = js.jobid;
		task2.stageid = js.stageid;
		function = new UnionFunction();
		js.stage.tasks.clear();
		js.stage.tasks.add(function);
		mdsti.setTask(task2);
		mdsti.processBlockHDFSUnion(bls2.get(0), bls2.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is1 = mdsti.getIntermediateInputStreamFS(task1.jobid + task1.stageid + task1.taskid);
		List<InputStream> istreams1 = Arrays.asList(is1);

		InputStream is2 = mdsti.getIntermediateInputStreamFS(task2.jobid + task2.stageid + task2.taskid);
		List<InputStream> istreams2 = Arrays.asList(is2);

		Task taskunion = new Task();
		taskunion.jobid = js.jobid;
		taskunion.stageid = js.stageid;
		function = new UnionFunction();
		js.stage.tasks.clear();
		js.stage.tasks.add(function);
		mdsti.setTask(taskunion);
		mdsti.processBlockHDFSUnion(istreams1, istreams2);
		is1.close();
		is2.close();

		InputStream is = mdsti.getIntermediateInputStreamFS(taskunion.jobid + taskunion.stageid + taskunion.taskid);
		List<String> uniondata = (List<String>) kryo.readClassAndObject(new Input(is));
		assertEquals(60, uniondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMap() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		List<String[]> mapfilterdata = (List<String[]>) kryo.readClassAndObject(new Input(is));
		assertEquals(45957, mapfilterdata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCount() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		Task calculatecounttask = new Task();
		calculatecounttask.jobid = js.jobid;
		calculatecounttask.stageid = js.stageid;
		MapFunction<String, String[]> map = (String str) -> str.split(MDCConstants.COMMA);
		PredicateSerializable<String[]> filter = (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(new CalculateCount());

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, calculatecounttask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(
				calculatecounttask.jobid + calculatecounttask.stageid + calculatecounttask.taskid);
		List<Long> mapfiltercountdata = (List<Long>) kryo.readClassAndObject(new Input(is));
		assertEquals(45957l, (long) mapfiltercountdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapSummaryStatistics() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, sstask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(sstask.jobid + sstask.stageid + sstask.taskid);
		List<IntSummaryStatistics> mapfilterssdata = (List<IntSummaryStatistics>) kryo
				.readClassAndObject(new Input(is));
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
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, maxtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(maxtask.jobid + maxtask.stageid + maxtask.taskid);
		List<Integer> mapfiltermaxdata = (List<Integer>) kryo.readClassAndObject(new Input(is));
		is.close();
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, mintask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(mintask.jobid + mintask.stageid + mintask.taskid);
		List<Integer> mapfiltermaxdata = (List<Integer>) kryo.readClassAndObject(new Input(is));
		is.close();
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, sumtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(sumtask.jobid + sumtask.stageid + sumtask.taskid);
		List<Integer> mapfiltermaxdata = (List<Integer>) kryo.readClassAndObject(new Input(is));
		is.close();
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, sdtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(sdtask.jobid + sdtask.stageid + sdtask.taskid);
		List<Double> mapfiltermaxdata = (List<Double>) kryo.readClassAndObject(new Input(is));
		is.close();
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, calcultecounttask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(
				calcultecounttask.jobid + calcultecounttask.stageid + calcultecounttask.taskid);
		List<Long> mapfiltercountdata = (List<Long>) kryo.readClassAndObject(new Input(is));
		assertEquals(45957l, (long) mapfiltercountdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecord() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get("ArrDelay"))
				&& !"NA".equals(csvrecord.get("ArrDelay"));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		List<CSVRecord> filterdata = (List<CSVRecord>) kryo.readClassAndObject(new Input(is));
		assertEquals(45957l, (long) filterdata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordSumaryStatistics() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, summarystaticstask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(
				summarystaticstask.jobid + summarystaticstask.stageid + summarystaticstask.taskid);
		List<IntSummaryStatistics> mapfilterssdata = (List<IntSummaryStatistics>) kryo
				.readClassAndObject(new Input(is));
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
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, maxtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(maxtask.jobid + maxtask.stageid + maxtask.taskid);
		List<Integer> mapfilterssdata = (List<Integer>) kryo.readClassAndObject(new Input(is));
		assertEquals(623, (long) mapfilterssdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordMin() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, mintask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(mintask.jobid + mintask.stageid + mintask.taskid);
		List<Integer> mapfilterssdata = (List<Integer>) kryo.readClassAndObject(new Input(is));
		assertEquals(-89, (long) mapfilterssdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordSum() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, sumtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(sumtask.jobid + sumtask.stageid + sumtask.taskid);
		List<Integer> mapfilterssdata = (List<Integer>) kryo.readClassAndObject(new Input(is));
		assertEquals(-63278, (long) mapfilterssdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordStandardDeviation() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, sdtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(sdtask.jobid + sdtask.stageid + sdtask.taskid);
		List<Double> mapfiltersddata = (List<Double>) kryo.readClassAndObject(new Input(is));
		assertEquals(1, (long) mapfiltersddata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVCount() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStreamFS(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		Task calcultecounttask = new Task();
		calcultecounttask.jobid = js.jobid;
		calcultecounttask.stageid = js.stageid;
		mdsti.setTask(calcultecounttask);
		js.stage.tasks.clear();
		js.stage.tasks.add(new CalculateCount());
		mdsti.processBlockHDFSMap(inputtocount);
		is.close();
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		is = mdsti.getIntermediateInputStreamFS(
				calcultecounttask.jobid + calcultecounttask.stageid + calcultecounttask.taskid);
		List<Long> csvreccount = (List<Long>) kryo.readClassAndObject(new Input(is));
		assertEquals(45957l, (long) csvreccount.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVSummaryStatistics() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStreamFS(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get("ArrDelay"));
		Task summarystaticstask = new Task();
		summarystaticstask.jobid = js.jobid;
		summarystaticstask.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(csvint);
		js.stage.tasks.add(new SummaryStatistics());
		mdsti.setTask(summarystaticstask);
		mdsti.processBlockHDFSMap(inputtocount);
		is.close();
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		is = mdsti.getIntermediateInputStreamFS(
				summarystaticstask.jobid + summarystaticstask.stageid + summarystaticstask.taskid);
		List<IntSummaryStatistics> mapfilterssdata = (List<IntSummaryStatistics>) kryo
				.readClassAndObject(new Input(is));
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
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStreamFS(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get("ArrDelay"));
		Task maxtask = new Task();
		maxtask.jobid = js.jobid;
		maxtask.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(csvint);
		js.stage.tasks.add(new Max());
		mdsti = new StreamPipelineTaskExecutorIgnite(js, maxtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.processBlockHDFSMap(inputtocount);
		is.close();
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		is = mdsti.getIntermediateInputStreamFS(maxtask.jobid + maxtask.stageid + maxtask.taskid);
		List<Integer> mapfiltermaxdata = (List<Integer>) kryo.readClassAndObject(new Input(is));
		assertEquals(623, (int) mapfiltermaxdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVMin() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStreamFS(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get("ArrDelay"));
		Task mintask = new Task();
		mintask.jobid = js.jobid;
		mintask.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(csvint);
		js.stage.tasks.add(new Min());
		mdsti = new StreamPipelineTaskExecutorIgnite(js, mintask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.processBlockHDFSMap(inputtocount);
		is.close();
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		is = mdsti.getIntermediateInputStreamFS(mintask.jobid + mintask.stageid + mintask.taskid);
		List<Integer> mapfiltermindata = (List<Integer>) kryo.readClassAndObject(new Input(is));
		assertEquals(-89, (int) mapfiltermindata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVSum() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStreamFS(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get("ArrDelay"));
		Task sumtask = new Task();
		sumtask.jobid = js.jobid;
		sumtask.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(csvint);
		js.stage.tasks.add(new Sum());
		mdsti = new StreamPipelineTaskExecutorIgnite(js, sumtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.processBlockHDFSMap(inputtocount);
		is.close();
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		is = mdsti.getIntermediateInputStreamFS(sumtask.jobid + sumtask.stageid + sumtask.taskid);
		List<Integer> mapfiltersumdata = (List<Integer>) kryo.readClassAndObject(new Input(is));
		assertEquals(-63278, (int) mapfiltersumdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVSD() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStreamFS(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		ToIntFunction<CSVRecord> csvint = (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get("ArrDelay"));
		Task sdtask = new Task();
		sdtask.jobid = js.jobid;
		sdtask.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(csvint);
		js.stage.tasks.add(new Sum());
		mdsti = new StreamPipelineTaskExecutorIgnite(js, sdtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.processBlockHDFSMap(inputtocount);
		is.close();
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		is = mdsti.getIntermediateInputStreamFS(sdtask.jobid + sdtask.stageid + sdtask.taskid);
		List<Integer> mapfiltersddata = (List<Integer>) kryo.readClassAndObject(new Input(is));
		assertEquals(1, (int) mapfiltersddata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessSample() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processSamplesBlocks(100, bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		List<Long> csvreccount = (List<Long>) kryo.readClassAndObject(new Input(is));
		assertEquals(100, (long) csvreccount.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessSampleCount() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task counttask = new Task();
		counttask.jobid = js.jobid;
		counttask.stageid = js.stageid;
		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		Object function = new CalculateCount();
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(function);
		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, counttask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processSamplesBlocks(150, bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(counttask.jobid + counttask.stageid + counttask.taskid);
		List<Long> csvreccount = (List<Long>) kryo.readClassAndObject(new Input(is));
		assertEquals(150l, (long) csvreccount.get(0));
		is.close();
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessStreamSample() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStreamFS(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		List<InputStream> inputtocount = Arrays.asList(is);
		Task sample = new Task();
		sample.jobid = js.jobid;
		sample.stageid = js.stageid;
		Function samplefn = val -> val;
		js.stage.tasks.clear();
		js.stage.tasks.add(samplefn);
		mdsti.setTask(sample);
		mdsti.processSamplesObjects(150, inputtocount);
		is.close();
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		is = mdsti.getIntermediateInputStreamFS(sample.jobid + sample.stageid + sample.taskid);
		List<Integer> mapfiltersddata = (List<Integer>) kryo.readClassAndObject(new Input(is));
		assertEquals(150, (int) mapfiltersddata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamSampleCount() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
		js.stage.tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;

		PredicateSerializable<CSVRecord> filter = (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.stage.tasks.add(csvoptions);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStreamFS(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		List<InputStream> inputtocount = Arrays.asList(is);
		Task counttask = new Task();
		counttask.jobid = js.jobid;
		counttask.stageid = js.stageid;
		Object count = new CalculateCount();
		js.stage.tasks.clear();
		js.stage.tasks.add(count);
		mdsti.setTask(counttask);
		mdsti.processSamplesObjects(150, inputtocount);
		is.close();
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		is = mdsti.getIntermediateInputStreamFS(counttask.jobid + counttask.stageid + counttask.taskid);
		List<Long> mapfiltersddata = (List<Long>) kryo.readClassAndObject(new Input(is));
		assertEquals(150l, (long) mapfiltersddata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessJoin() throws Exception {
		JobStage js = new JobStage();
		js.stage = new Stage();
		js.jobid = MDCConstants.JOB + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stageid = MDCConstants.STAGE + MDCConstants.HYPHEN + System.currentTimeMillis();
		js.stage.id = js.stageid;
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, reducebykeytask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Task reducebykeytask2 = new Task();
		reducebykeytask2.jobid = js.jobid;
		reducebykeytask2.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);
		js.stage.tasks.add(redfunc);
		mdsti = new StreamPipelineTaskExecutorIgnite(js, reducebykeytask2);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.setTask(reducebykeytask2);
		mdsti.processBlockHDFSMap(bls2.get(0), hdfs);

		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is1 = mdsti.getIntermediateInputStreamFS(
				reducebykeytask1.jobid + reducebykeytask1.stageid + reducebykeytask1.taskid);
		InputStream is2 = mdsti.getIntermediateInputStreamFS(
				reducebykeytask2.jobid + reducebykeytask2.stageid + reducebykeytask2.taskid);
		Task jointask = new Task();
		jointask.jobid = js.jobid;
		jointask.stageid = js.stageid;
		js.stage.tasks.clear();
		Consumer<String> dummy = val -> {
		};
		js.stage.tasks.add(dummy);
		JoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdsti = new StreamPipelineTaskExecutorIgnite(js, jointask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.processJoinLZF(is1, is2, jp, false, false);
		is1.close();
		is2.close();

		kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(jointask.jobid + jointask.stageid + jointask.taskid);
		List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> mapfiltersddata = (List) kryo
				.readClassAndObject(new Input(is));
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, reducebykeytask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		reducebykeytask2.jobid = js.jobid;
		reducebykeytask2.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);
		js.stage.tasks.add(redfunc);
		mdsti = new StreamPipelineTaskExecutorIgnite(js, reducebykeytask2);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.processBlockHDFSMap(bls2.get(0), hdfs);

		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is1 = mdsti.getIntermediateInputStreamFS(
				reducebykeytask1.jobid + reducebykeytask1.stageid + reducebykeytask1.taskid);
		InputStream is2 = mdsti.getIntermediateInputStreamFS(
				reducebykeytask2.jobid + reducebykeytask2.stageid + reducebykeytask2.taskid);
		Task jointask = new Task();
		jointask.jobid = js.jobid;
		jointask.stageid = js.stageid;
		js.stage.tasks.clear();
		Consumer<String> dummy = val -> {
		};
		js.stage.tasks.add(dummy);
		LeftOuterJoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdsti = new StreamPipelineTaskExecutorIgnite(js, jointask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.processLeftOuterJoinLZF(is1, is2, jp, false, false);
		is1.close();
		is2.close();

		kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(jointask.jobid + jointask.stageid + jointask.taskid);
		List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> mapfiltersddata = (List) kryo
				.readClassAndObject(new Input(is));
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, reducebykeytask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		reducebykeytask2.jobid = js.jobid;
		reducebykeytask2.stageid = js.stageid;

		js.stage.tasks.clear();
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);
		js.stage.tasks.add(redfunc);
		mdsti = new StreamPipelineTaskExecutorIgnite(js, reducebykeytask2);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.processBlockHDFSMap(bls2.get(0), hdfs);

		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is1 = mdsti.getIntermediateInputStreamFS(
				reducebykeytask1.jobid + reducebykeytask1.stageid + reducebykeytask1.taskid);
		InputStream is2 = mdsti.getIntermediateInputStreamFS(
				reducebykeytask2.jobid + reducebykeytask2.stageid + reducebykeytask2.taskid);
		Task jointask = new Task();
		jointask.jobid = js.jobid;
		jointask.stageid = js.stageid;
		mdsti.setTask(jointask);
		js.stage.tasks.clear();

		Consumer<String> dummy = val -> {
		};
		js.stage.tasks.add(dummy);
		RightOuterJoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdsti.processRightOuterJoinLZF(is2, is1, jp, false, false);
		is1.close();
		is2.close();

		kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(jointask.jobid + jointask.stageid + jointask.taskid);
		List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> mapfiltersddata = (List) kryo
				.readClassAndObject(new Input(is));
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, mappairtask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is1 = mdsti
				.getIntermediateInputStreamFS(mappairtask1.jobid + mappairtask1.stageid + mappairtask1.taskid);
		Task gbktask = new Task();
		gbktask.jobid = js.jobid;
		gbktask.stageid = js.stageid;
		js.stage.tasks.clear();
		Consumer<String> dummy = val -> {
		};
		js.stage.tasks.add(dummy);
		gbktask.input = new Object[]{is1};
		mdsti = new StreamPipelineTaskExecutorIgnite(js, gbktask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.processGroupByKeyTuple2();
		is1.close();

		kryo = Utils.getKryoNonDeflateSerializer();

		InputStream is = mdsti.getIntermediateInputStreamFS(gbktask.jobid + gbktask.stageid + gbktask.taskid);
		List<Tuple2<String, List<Integer>>> mapfiltersddata = (List) kryo.readClassAndObject(new Input(is));
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, mappairtask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is1 = mdsti
				.getIntermediateInputStreamFS(mappairtask1.jobid + mappairtask1.stageid + mappairtask1.taskid);
		Task fbktask = new Task();
		fbktask.jobid = js.jobid;
		fbktask.stageid = js.stageid;
		js.stage.tasks.clear();
		ReduceByKeyFunction<Long> redfunc = (a, b) -> a + b;
		FoldByKey fbk = new FoldByKey(0l, redfunc, true);
		js.stage.tasks.add(fbk);
		fbktask.input = new Object[]{is1};
		mdsti.setTask(fbktask);
		mdsti.processFoldByKeyTuple2();
		is1.close();

		kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(fbktask.jobid + fbktask.stageid + fbktask.taskid);
		List<Tuple2<String, Long>> mapfiltersddata = (List) kryo.readClassAndObject(new Input(is));

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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, mappairtask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		Kryo kryo = Utils.getKryoNonDeflateSerializer();

		InputStream is1 = mdsti
				.getIntermediateInputStreamFS(mappairtask1.jobid + mappairtask1.stageid + mappairtask1.taskid);
		Task fbktask = new Task();
		fbktask.jobid = js.jobid;
		fbktask.stageid = js.stageid;
		js.stage.tasks.clear();
		ReduceByKeyFunction<Long> redfunc = (a, b) -> a + b;
		FoldByKey fbk = new FoldByKey(0l, redfunc, false);
		js.stage.tasks.add(fbk);
		fbktask.input = new Object[]{is1};
		mdsti = new StreamPipelineTaskExecutorIgnite(js, fbktask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.processFoldByKeyTuple2();
		is1.close();

		kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(fbktask.jobid + fbktask.stageid + fbktask.taskid);
		List<Tuple2<String, Long>> mapfiltersddata = (List) kryo.readClassAndObject(new Input(is));

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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, mappairtask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is1 = mdsti
				.getIntermediateInputStreamFS(mappairtask1.jobid + mappairtask1.stageid + mappairtask1.taskid);
		Task cbktask = new Task();
		js.stage.tasks.clear();
		js.stage.tasks.add(new CountByKeyFunction());
		cbktask.input = new Object[]{is1};
		mdsti = new StreamPipelineTaskExecutorIgnite(js, cbktask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.setTask(cbktask);
		mdsti.processCountByKeyTuple2();
		is1.close();

		kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(cbktask.jobid + cbktask.stageid + cbktask.taskid);
		List<Tuple2<String, Long>> mapfiltersddata = (List) kryo.readClassAndObject(new Input(is));

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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, mappairtask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is1 = mdsti
				.getIntermediateInputStreamFS(mappairtask1.jobid + mappairtask1.stageid + mappairtask1.taskid);
		Task cbktask = new Task();
		cbktask.jobid = js.jobid;
		cbktask.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(new CountByKeyFunction());
		cbktask.input = new Object[]{is1};
		mdsti = new StreamPipelineTaskExecutorIgnite(js, cbktask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.processCountByValueTuple2();
		is1.close();

		kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(cbktask.jobid + cbktask.stageid + cbktask.taskid);
		List<Tuple2<Tuple2<String, Long>, Long>> mapfiltersddata = (List) kryo.readClassAndObject(new Input(is));
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

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, reducebykeytask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		reducebykeytask2.jobid = js.jobid;
		reducebykeytask2.stageid = js.stageid;
		js.stage.tasks.clear();
		js.stage.tasks.add(map);
		js.stage.tasks.add(filter);
		js.stage.tasks.add(pair);
		js.stage.tasks.add(redfunc);
		mdsti = new StreamPipelineTaskExecutorIgnite(js, reducebykeytask2);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.processBlockHDFSMap(bls2.get(0), hdfs);

		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is1 = mdsti.getIntermediateInputStreamFS(
				reducebykeytask1.jobid + reducebykeytask1.stageid + reducebykeytask1.taskid);
		InputStream is2 = mdsti.getIntermediateInputStreamFS(
				reducebykeytask2.jobid + reducebykeytask2.stageid + reducebykeytask2.taskid);
		Task coalescetask = new Task();
		coalescetask.jobid = js.jobid;
		coalescetask.stageid = js.stageid;
		js.stage.tasks.clear();
		Coalesce<Integer> coalesce = new Coalesce();
		coalesce.coalescepartition = 1;
		coalesce.coalescefuncion = (a, b) -> a + b;
		js.stage.tasks.add(coalesce);
		coalescetask.input = new Object[]{is1, is2};
		RightOuterJoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdsti = new StreamPipelineTaskExecutorIgnite(js, coalescetask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.processCoalesce();
		is1.close();
		is2.close();

		kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti
				.getIntermediateInputStreamFS(coalescetask.jobid + coalescetask.stageid + coalescetask.taskid);
		List<Tuple2<String, Integer>> mapfiltersddata = (List) kryo.readClassAndObject(new Input(is));
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
		js.stage.tasks = new ArrayList<>();
		Json json = new Json();
		Task filtertask = new Task();
		filtertask.jobid = js.jobid;
		filtertask.stageid = js.stageid;
		PredicateSerializable<JSONObject> filter = jsonobj -> jsonobj != null
				&& jsonobj.get("type").equals("CreateEvent");
		js.stage.tasks.add(json);
		js.stage.tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(js, filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
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
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Kryo kryo = Utils.getKryoNonDeflateSerializer();
		InputStream is = mdsti.getIntermediateInputStreamFS(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		List<JSONObject> jsonfilterdata = (List<JSONObject>) kryo.readClassAndObject(new Input(is));
		assertEquals(11l, (long) jsonfilterdata.size());
		is.close();
	}

	// JSON Test cases End
}
