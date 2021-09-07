package com.github.mdc.stream.ignite;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.github.mdc.stream.MapPairIgnite;
import com.github.mdc.stream.MassiveDataPipelineIgnite;

@SuppressWarnings({ "rawtypes" })
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MassiveDataPipelineIgniteFileTest  extends MassiveDataPipelineIgniteBase {
	boolean toexecute = true;
	String airlinesfolder = "file:E:/DEVELOPMENT/dataset/airline";
	String carriersfolder = "file:E:/DEVELOPMENT/dataset/carriers";
	@SuppressWarnings("unchecked")
	@Test
	public void testMapFilterIgnite() throws Throwable {
		log.info("testMapFilterIgnite Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setBlocksize("64");
		pipelineconfig.setIsblocksuserdefined("false");
		MassiveDataPipelineIgnite<String> datapipeline = MassiveDataPipelineIgnite.newStream(airlinesfolder,
				pipelineconfig);
		MassiveDataPipelineIgnite<String[]> mdpi = datapipeline.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !dat[14].equals("ArrDelay") && !dat[14].equals("NA")).cache(false);
		MassiveDataPipelineIgnite<Tuple2<String,Integer>> tupresult = mdpi.map(dat->new Tuple2<String,Integer>(dat[8],Integer.parseInt(dat[14]))).cache(true);
		int sum = 0;
		List<List> results = ((List)((List)tupresult.job.results));
		sum = 0;
		for(List result:results) {
			sum+=result.size(); 
		}
		assertEquals(1288326, sum);
		MassiveDataPipelineIgnite<Tuple2<String, Integer>> tupresult1 = mdpi.map(dat->new Tuple2<String,Integer>(dat[0],Integer.parseInt(dat[14]))).cache(true);
		results = ((List)((List)tupresult1.job.results));
		sum = 0;
		for(List result:results) {
			sum+=result.size(); 
		}
		assertEquals(1288326, sum);
		log.info("testMapFilterIgnite After---------------------------------------");
	}
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testMapFilterMapPairRbkIgniteJoin() throws Throwable {
		log.info("testMapFilterMapPairRbkIgniteJoin Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setBlocksize("64");
		pipelineconfig.setIsblocksuserdefined("false");
		MassiveDataPipelineIgnite<String> datastream = MassiveDataPipelineIgnite.newStream(airlinesfolder,
				pipelineconfig);
		MapPairIgnite<String, Integer> mti = datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !dat[14].equals("ArrDelay") && !dat[14].equals("NA")).mapToPair(dat->new Tuple2<String,Integer>(dat[8],Integer.parseInt(dat[14]))).cache(false);
		MapPairIgnite<String, Integer> tupresult = mti.reduceByKey((a,b)->a+b).coalesce(1, (a,b)->a+b).cache(true);
		assertEquals(14, ((List)((List)tupresult.job.results).get(0)).size());
		MapPairIgnite<String, Integer> tupresult1 = mti.reduceByKey((a,b)->a+b).coalesce(1, (a,b)->a+b).cache(true);
		assertEquals(14, ((List)((List)tupresult1.job.results).get(0)).size());
		MapPairIgnite<Tuple2<String,Integer>, Tuple2<String,Integer>> joinresult = (MapPairIgnite) tupresult.join(tupresult1, (tup1,tup2)->tup1.v1.equals(tup2.v1)).cache(true);
		assertEquals(14, ((List)((List)joinresult.job.results).get(0)).size());
		log.info("testMapFilterMapPairRbkIgniteJoin After---------------------------------------");
	}
	
	
	@Test
	@SuppressWarnings({ "unchecked" })
	public void testReduceByKeyCoalesceJoinUserDefinedBlockSize() throws Throwable {
		log.info("testReduceByKeyCoalesceJoinUserDefinedBlockSize Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("256");
		MassiveDataPipelineIgnite<String> datastream = MassiveDataPipelineIgnite.newStream(airlinesfolder,
				pipelineconfig);
		MapPairIgnite<String, Long> mappair1 = (MapPairIgnite) datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPairIgnite<String, Long> airlinesamples = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		MassiveDataPipelineIgnite<String> datastream1 = MassiveDataPipelineIgnite.newStream(carriersfolder, pipelineconfig);

		MapPairIgnite<Tuple, Object> carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		List<List> results = (List<List>) carriers
				.join(airlinesamples, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1)).collect(true, null);
		log.info(results);
		log.info("testReduceByKeyCoalesceJoinUserDefinedBlockSize After---------------------------------------");
	}
}
