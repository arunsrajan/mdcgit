package com.github.mdc.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Test;

import com.github.mdc.stream.MapPair;
import com.github.mdc.stream.MassiveDataPipeline;

public class MassiveDataPipelineCoalesceTest extends MassiveDataPipelineBaseTestClassCommon {
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testCoalesce() throws Throwable {
		String pipelineconfigblocksize = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		log.info("testCoalesce Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List> coalesceresult = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2).collect(true, null);

		long sum = 0;
		for (List<Tuple> tuples : coalesceresult) {
			for (Tuple tuple : tuples) {
				log.info(tuple);
				sum += (long) ((Tuple2) tuple).v2;
			}
			log.info("");
		}
		assertEquals(-63278, sum);
		pipelineconfig.setBlocksize(pipelineconfigblocksize);

		log.info("testCoalesce After---------------------------------------");
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCoalesceWithJoin() throws Throwable {
		String pipelineconfigblocksize = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		log.info("testCoalesceWithJoin Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String,Long> coalesce = (MapPair<String,Long>) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));
		
		MapPair<String,Long> coalesce1 = coalesce
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2);
		
		MapPair<String,Long> coalesce2 = coalesce
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2);
		
		List<List<Tuple>> coalesceresult = coalesce1.join(coalesce2, (dat1, dat2) -> dat1.equals(dat2)).collect(true, null);

		long sum = 0;
		for (List<Tuple> tuples : coalesceresult) {
			for (Tuple tuple : tuples) {
				log.info(tuple);
				sum += (long) ((Tuple2)((Tuple2) tuple).v2).v2;
			}
			log.info("");
		}
		assertEquals(-63278, sum);
		pipelineconfig.setBlocksize(pipelineconfigblocksize);

		log.info("testCoalesceWithJoin After---------------------------------------");
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyCoalesceWithJoin() throws Throwable {
		String pipelineconfigblocksize = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		log.info("testReduceByKeyCoalesceWithJoin Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String,Long> coalesce = (MapPair<String,Long>) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));
		
		MapPair<String,Long> coalesce1 = coalesce
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2);
		
		MapPair<String,Long> coalesce2 = coalesce
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2);
		
		List<List<Tuple2<Tuple2<String,Object>,Tuple2<String,Object>>>> coalesceresult = coalesce1.join(coalesce2, (dat1, dat2) -> ((Tuple2)dat1).v1.equals(((Tuple2)dat2).v1)).collect(true, null);

		for (List<Tuple2<Tuple2<String,Object>,Tuple2<String,Object>>> tuples : coalesceresult) {
			for (Tuple2<Tuple2<String,Object>,Tuple2<String,Object>> tuple : tuples) {
				assertTrue(tuple.v1.v1.equals(tuple.v2.v1));
				log.info(tuple);
			}
			log.info("");
		}
		pipelineconfig.setBlocksize(pipelineconfigblocksize);

		log.info("testReduceByKeyCoalesceWithJoin After---------------------------------------");
	}
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCoalesceReduceByKeyWithJoin() throws Throwable {
		String pipelineconfigblocksize = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		log.info("testCoalesceReduceByKeyWithJoin Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String,Long> coalesce = (MapPair<String,Long>) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));
		
		MapPair<String,Long> coalesce1 = coalesce
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2);
		
		MapPair<String,Long> coalesce2 = coalesce
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2);
		
		List<List<Tuple2<Tuple2<String,Object>,Tuple2<String,Object>>>> coalesceresult = coalesce2.join(coalesce1, (dat1, dat2) -> ((Tuple2)dat1).v1.equals(((Tuple2)dat2).v1)).collect(true, null);

		for (List<Tuple2<Tuple2<String,Object>,Tuple2<String,Object>>> tuples : coalesceresult) {
			for (Tuple2<Tuple2<String,Object>,Tuple2<String,Object>> tuple : tuples) {
				assertTrue(tuple.v1.v1.equals(tuple.v2.v1));
				log.info(tuple);
			}
			log.info("");
		}
		pipelineconfig.setBlocksize(pipelineconfigblocksize);

		log.info("testCoalesceReduceByKeyWithJoin After---------------------------------------");
	}
}
