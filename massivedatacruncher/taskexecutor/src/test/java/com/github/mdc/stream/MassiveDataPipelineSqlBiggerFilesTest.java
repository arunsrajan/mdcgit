package com.github.mdc.stream;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.github.mdc.common.MDCConstants.STORAGE;
import com.github.mdc.stream.sql.MDPSql;
import com.github.mdc.stream.sql.MDPSqlBuilder;

public class MassiveDataPipelineSqlBiggerFilesTest extends MassiveDataPipelineBaseTestClassCommon {
	String[] airlineheader = new String[] { "AirlineYear", "MonthOfYear", "DayofMonth", "DayOfWeek", "DepTime",
			"CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime",
			"CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut",
			"Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay" };
	String[] carrierheader = { "Code", "Description" };
	Logger log = Logger.getLogger(MassiveDataPipelineSqlBiggerFilesTest.class);

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testMDPSqlBuilderSumArrivalDelayInMemory() throws Exception {
		pipelineconfig.setLocal("false");
		pipelineconfig.setStorage(STORAGE.INMEMORY);
		pipelineconfig.setBlocksize("64");
		pipelineconfig.setIsblocksuserdefined("true");
		log.info("In testMDPSqlBuilderSumArrivalDelayInMemory() method Entry");
		String statement = "SELECT sum(ArrDelay) "
				+ "FROM airline where ArrDelay<>'ArrDelay' and ArrDelay<>'NA'";
		MDPSql mdpsql = MDPSqlBuilder.newBuilder().add("/1987", "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Long>> records = (List<List<Long>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Long> recs : records) {
			for (Long rec : recs) {
				log.info(rec);
				sum += rec;
			}
		}
		assertEquals(12170428, sum);
		log.info("In testMDPSqlBuilderSumArrivalDelayInMemory() method Exit");
		pipelineconfig.setLocal("true");
	}
	
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testMDPSqlBuilderSumArrivalDelayInMemoryLocal() throws Exception {
		pipelineconfig.setLocal("true");
		pipelineconfig.setBlocksize("32");
		pipelineconfig.setIsblocksuserdefined("true");
		log.info("In testMDPSqlBuilderSumArrivalDelayInMemoryLocal() method Entry");
		String statement = "SELECT sum(ArrDelay) "
				+ "FROM airline where ArrDelay<>'ArrDelay' and ArrDelay<>'NA'";
		MDPSql mdpsql = MDPSqlBuilder.newBuilder().add("/1987", "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Long>> records = (List<List<Long>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Long> recs : records) {
			for (Long rec : recs) {
				log.info(rec);
				sum += rec;
			}
		}
		assertEquals(12170428, sum);
		log.info("In testMDPSqlBuilderSumArrivalDelayInMemoryLocal() method Exit");
		pipelineconfig.setLocal("true");
	}
	
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testMDPSqlBuilderCountArrivalDelayBiggerFiles() throws Exception {
		pipelineconfig.setLocal("false");
		pipelineconfig.setBatchsize("5");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setStorage(STORAGE.INMEMORY_DISK);
		log.info("In testMDPSqlBuilderCountArrivalDelayBiggerFiles() method Entry");
		String statement = "SELECT count(ArrDelay) "
				+ "FROM airline where ArrDelay<>'ArrDelay' and ArrDelay<>'NA'";
		MDPSql mdpsql = MDPSqlBuilder.newBuilder().add(airlines, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Long>> records = (List<List<Long>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Long> recs : records) {
			for (Long rec : recs) {
				log.info(rec);
				sum += rec.longValue();
			}
		}
		assertEquals(120947440l, sum);
		log.info("In testMDPSqlBuilderCountArrivalDelayBiggerFiles() method Exit");
	}
	
	
	@SuppressWarnings({ "unchecked" })
	@Test
	public void testMDPSqlBuilderSumArrivalDelayDisk() throws Exception {
		pipelineconfig.setLocal("false");
		pipelineconfig.setStorage(STORAGE.DISK);
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setBatchsize("2");
		pipelineconfig.setIsblocksuserdefined("true");
		log.info("In testMDPSqlBuilderSumArrivalDelayDisk() method Entry");
		String statement = "SELECT sum(ArrDelay) "
				+ "FROM airline where ArrDelay<>'ArrDelay' and ArrDelay<>'NA'";
		MDPSql mdpsql = MDPSqlBuilder.newBuilder().add("/1987", "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Long>> records = (List<List<Long>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Long> recs : records) {
			for (Long rec : recs) {
				log.info(rec);
				sum += rec;
			}
		}
		assertEquals(12170428, sum);
		log.info("In testMDPSqlBuilderSumArrivalDelayDisk() method Exit");
		pipelineconfig.setLocal("true");
	}
}
