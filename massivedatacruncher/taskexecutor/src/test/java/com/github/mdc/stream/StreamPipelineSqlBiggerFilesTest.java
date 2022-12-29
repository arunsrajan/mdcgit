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
package com.github.mdc.stream;

import static org.junit.Assert.assertEquals;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.junit.Test;
import com.github.mdc.common.MDCConstants.STORAGE;
import com.github.mdc.stream.sql.StreamPipelineSql;
import com.github.mdc.stream.sql.StreamPipelineSqlBuilder;

public class StreamPipelineSqlBiggerFilesTest extends StreamPipelineBaseTestCommon {
	String[] airlineheader = new String[]{"AirlineYear", "MonthOfYear", "DayofMonth", "DayOfWeek", "DepTime",
			"CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime",
			"CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut",
			"Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay"};
	String[] carrierheader = {"Code", "Description"};
	Logger log = Logger.getLogger(StreamPipelineSqlBiggerFilesTest.class);

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderSumArrivalDelayInMemory() throws Exception {
		pipelineconfig.setLocal("false");
		pipelineconfig.setStorage(STORAGE.INMEMORY);
		pipelineconfig.setBlocksize("64");
		pipelineconfig.setIsblocksuserdefined("true");
		log.info("In testMDPSqlBuilderSumArrivalDelayInMemory() method Entry");
		String statement = "SELECT sum(ArrDelay) "
				+ "FROM airline where ArrDelay<>'ArrDelay' and ArrDelay<>'NA'";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add("/1987", "airline", airlineheader, airsqltype)
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


	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderSumArrivalDelayInMemoryLocal() throws Exception {
		pipelineconfig.setLocal("true");
		pipelineconfig.setBlocksize("32");
		pipelineconfig.setIsblocksuserdefined("true");
		log.info("In testMDPSqlBuilderSumArrivalDelayInMemoryLocal() method Entry");
		String statement = "SELECT sum(ArrDelay) "
				+ "FROM airline where ArrDelay<>'ArrDelay' and ArrDelay<>'NA'";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add("/1987", "airline", airlineheader, airsqltype)
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


	@SuppressWarnings({"unchecked"})
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
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlines, "airline", airlineheader, airsqltype)
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

	@SuppressWarnings({"unchecked"})
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
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add("/1987", "airline", airlineheader, airsqltype)
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
	
	
	
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testSqlUniqueCarrierSumCountArrDelaySAInMemory() throws Exception {
		pipelineconfig.setLocal("false");
		pipelineconfig.setStorage(STORAGE.INMEMORY);
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setBatchsize("2");
		pipelineconfig.setIsblocksuserdefined("true");
		log.info("In testSqlUniqueCarrierSumCountArrDelaySAInMemory() method Entry");
		String statement = "SELECT UniqueCarrier,sum(ArrDelay),count(ArrDelay) "
				+ "FROM airline where ArrDelay<>'NA' and ArrDelay<>'ArrDelay' group by UniqueCarrier";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlines, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sum += (Long) rec.get("sum(ArrDelay)");
			}
		}
		log.info("Sum = " + sum);
		log.info("In testSqlUniqueCarrierSumCountArrDelaySAInMemory() method Exit");
		pipelineconfig.setLocal("true");
	}
	
	
}
