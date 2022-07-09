package com.github.mdc.stream;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.jooq.lambda.tuple.Tuple2;
import org.junit.Test;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.stream.MapPairIgnite;
import com.github.mdc.stream.IgnitePipeline;

public class StreamPipelineIgniteTest extends StreamPipelineBaseTestCommon {
	@SuppressWarnings("rawtypes")
	@Test
	public void testMapFilterIgnite() throws Throwable {
		log.info("testMapFilterIgnite Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setIsblocksuserdefined("false");
		pipelineconfig.setMode(MDCConstants.MODE_DEFAULT);
		IgnitePipeline<String> datastream = IgnitePipeline.newStreamHDFS(hdfsfilepath, "/airlinesample",
				pipelineconfig);
		IgnitePipeline<String[]> mdpi = datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).cache(false);
		IgnitePipeline<Tuple2<String, Integer>> tupresult = mdpi.map(dat -> new Tuple2<String, Integer>(dat[8], Integer.parseInt(dat[14]))).cache(true);
		assertEquals(45957, ((List) ((List) tupresult.job.results).get(0)).size());
		IgnitePipeline<Tuple2<String, Integer>> tupresult1 = mdpi.map(dat -> new Tuple2<String, Integer>(dat[0], Integer.parseInt(dat[14]))).cache(true);
		assertEquals(45957, ((List) ((List) tupresult1.job.results).get(0)).size());
		log.info("testMapFilterIgnite After---------------------------------------");
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testMapFilterMapPairRbkIgnite() throws Throwable {
		log.info("testMapFilterMapPairRbkIgnite Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setIsblocksuserdefined("false");
		pipelineconfig.setMode(MDCConstants.MODE_DEFAULT);
		IgnitePipeline<String> datastream = IgnitePipeline.newStreamHDFS(hdfsfilepath, "/1987",
				pipelineconfig);
		IgnitePipeline<String[]> mdpi = datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).cache(false);
		MapPairIgnite<String, Integer> tupresult = mdpi.mapToPair(dat -> new Tuple2<String, Integer>(dat[8], Integer.parseInt(dat[14]))).reduceByKey((a, b) -> a + b).cache(true);
		assertEquals(14, ((List) ((List) tupresult.job.results).get(0)).size());
		MapPairIgnite<String, Integer> tupresult1 = mdpi.mapToPair(dat -> new Tuple2<String, Integer>(dat[0], Integer.parseInt(dat[14]))).reduceByKey((a, b) -> a + b).cache(true);
		assertEquals(1, ((List) ((List) tupresult1.job.results).get(0)).size());
		log.info("testMapFilterMapPairRbkIgnite After---------------------------------------");
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testMapFilterMapTupeRbkIgniteJoin() throws Throwable {
		log.info("testMapFilterMapTupeRbkIgnite Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setIsblocksuserdefined("false");
		pipelineconfig.setMode(MDCConstants.MODE_DEFAULT);
		IgnitePipeline<String> datastream = IgnitePipeline.newStreamHDFS(hdfsfilepath, "/1987",
				pipelineconfig);
		MapPairIgnite<String, Integer> mti = datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> new Tuple2<String, Integer>(dat[8], Integer.parseInt(dat[14]))).cache(false);
		MapPairIgnite<String, Integer> tupresult = mti.reduceByKey((a, b) -> a + b).cache(true);
		assertEquals(14, ((List) ((List) tupresult.job.results).get(0)).size());
		MapPairIgnite<String, Integer> tupresult1 = mti.reduceByKey((a, b) -> a + b).cache(true);
		assertEquals(14, ((List) ((List) tupresult1.job.results).get(0)).size());
		MapPairIgnite<Tuple2<String, Integer>, Tuple2<String, Integer>> joinresult = (MapPairIgnite) tupresult.join(tupresult1, (tup1, tup2) -> tup1.v1.equals(tup2.v1)).cache(true);
		assertEquals(14, ((List) ((List) joinresult.job.results).get(0)).size());
		log.info("testMapFilterMapTupeRbkIgnite After---------------------------------------");
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testMapFilterMapTupeRbkIgniteBigJoin() throws Throwable {
		log.info("testMapFilterMapTupeRbkIgniteBigJoin Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("64");
		pipelineconfig.setMode(MDCConstants.MODE_DEFAULT);
		IgnitePipeline<String> datastream = IgnitePipeline.newStreamHDFS(hdfsfilepath, "/1987",
				pipelineconfig);
		MapPairIgnite<String, Integer> mti = datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> new Tuple2<String, Integer>(dat[8], Integer.parseInt(dat[14]))).cache(false);
		MapPairIgnite<String, Integer> tupresult = mti.reduceByKey((a, b) -> a + b).cache(true);
		assertEquals(14, ((List) ((List) tupresult.job.results).get(0)).size());
		MapPairIgnite<String, Integer> tupresult1 = mti.reduceByKey((a, b) -> a + b).cache(true);
		assertEquals(14, ((List) ((List) tupresult1.job.results).get(0)).size());
		MapPairIgnite<Tuple2<String, Integer>, Tuple2<String, Integer>> joinresult = (MapPairIgnite) tupresult.join(tupresult1, (tup1, tup2) -> tup1.v1.equals(tup2.v1)).cache(true);
		assertEquals(4, (((List) joinresult.job.results).size()));
		log.info("testMapFilterMapTupeRbkIgniteBigJoin After---------------------------------------");
	}


	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testMapFilterMapTupeRbkIgniteBigJoinCoalesce() throws Throwable {
		log.info("testMapFilterMapTupeRbkIgnite Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("64");
		pipelineconfig.setMode(MDCConstants.MODE_DEFAULT);
		IgnitePipeline<String> datastream = IgnitePipeline.newStreamHDFS(hdfsfilepath, "/1987",
				pipelineconfig);
		MapPairIgnite<String, Integer> mti = datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> new Tuple2<String, Integer>(dat[8], Integer.parseInt(dat[14]))).cache(false);
		MapPairIgnite<String, Integer> tupresult = mti.reduceByKey((a, b) -> a + b).coalesce(1, (a, b) -> a + b).cache(true);
		assertEquals(14, ((List) ((List) tupresult.job.results).get(0)).size());
		MapPairIgnite<String, Integer> tupresult1 = mti.reduceByKey((a, b) -> a + b).coalesce(1, (a, b) -> a + b).cache(true);
		assertEquals(14, ((List) ((List) tupresult1.job.results).get(0)).size());
		MapPairIgnite<Tuple2<String, Integer>, Tuple2<String, Integer>> joinresult = (MapPairIgnite) tupresult.join(tupresult1, (tup1, tup2) -> tup1.v1.equals(tup2.v1)).cache(true);
		assertEquals(14, ((List) ((List) joinresult.job.results).get(0)).size());
		log.info("testMapFilterMapTupeRbkIgnite After---------------------------------------");
	}
}
