package com.github.mdc.stream;

import static org.junit.Assert.assertNotNull;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Test;

import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.ByteBufferPool;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCNodesResources;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Resources;
import com.github.mdc.common.Utils;

public class LaunchContainersTest extends StreamPipelineBaseTestCommon {
	
	@Test
	public void testLaunchContainersDestroy() throws Exception {
		PipelineConfig pc = new PipelineConfig();
		Utils.loadLog4JSystemPropertiesClassPath(MDCConstants.MDC_TEST_PROPERTIES);
		pc.setBlocksize("64");
		pc.setNumberofcontainers("1");
		pc.setMaxmem("1024");
		pc.setMinmem("1024");
		pc.setLocal("true");
		pc.setJgroups("false");
		pc.setMesos("false");
		pc.setYarn("false");
		pc.setOutput(new Output(System.out));
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("64");
		pc.setMode(MDCConstants.MODE_NORMAL);
		Resources resources = new Resources();
		resources.setNumberofprocessors(12);
		resources.setFreememory(4294967296l);
		ConcurrentMap<String, Resources> mapres = new ConcurrentHashMap<>();
		mapres.put("127.0.0.1_12121", resources);
		resources.setNodeport("127.0.0.1_12121");
		MDCNodesResources.put(mapres);
		String containerid = Utils.launchContainers(1);
		assertNotNull(containerid);
		Utils.destroyContainers(containerid);
	}

	@Test
	public void testTELauncherJobSubmit() throws Exception {
		PipelineConfig pc = new PipelineConfig();
		pc.setBlocksize("64");
		pc.setNumberofcontainers("1");
		pc.setMaxmem("1024");
		pc.setMinmem("1024");
		pc.setJgroups("false");
		pc.setMesos("false");
		pc.setYarn("false");
		pc.setOutput(new Output(System.out));
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("64");
		pc.setMode(MDCConstants.MODE_NORMAL);
		Resources resources = new Resources();
		resources.setNumberofprocessors(12);
		resources.setFreememory(4294967296l);
		ConcurrentMap<String, Resources> mapres = new ConcurrentHashMap<>();
		mapres.put("127.0.0.1_12121", resources);
		resources.setNodeport("127.0.0.1_12121");
		MDCNodesResources.put(mapres);
		var lc = Utils.launchContainers(1);
		assertNotNull(lc);
		ByteBufferPoolDirect.init();
		ByteBufferPool.init(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.BYTEBUFFERPOOL_MAX, MDCConstants.BYTEBUFFERPOOL_MAX_DEFAULT)));
		pc.setLocal("false");
		pc.setUseglobaltaskexecutors(true);
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS("hdfs://127.0.0.1:9000", "/airline1989", pc);
		List<List<Tuple2>> joinresult = (List) datastream.map(dat -> dat.split(",")).filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14]))).mapValues(mv -> new Tuple2<Long, Long>(mv, 1l)).reduceByValues((tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2)).coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2)).collect(true, null);
		joinresult.stream().forEach(log::info);
		datastream.map(dat -> dat.split(",")).filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14]))).mapValues(mv -> new Tuple2<Long, Long>(mv, 1l)).reduceByValues((tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2)).coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2)).saveAsTextFile(new URI("hdfs://127.0.0.1:9000"), "/Coalesce/Coalesce-" + System.currentTimeMillis());
		MapPair<String, Tuple2<Long, Long>> mstll = datastream.map(dat -> dat.split(",")).filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14]))).mapValues(mv -> new Tuple2<Long, Long>(mv, 1l)).reduceByValues((tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2)).coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2));
		joinresult = mstll.collect(true, null);
		joinresult.stream().forEach(log::info);
		Utils.destroyContainers(lc);
	}
	
	
	
}
