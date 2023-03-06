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
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.github.mdc.common.PipelineConfig;
import com.github.mdc.stream.sql.build.StreamPipelineSql;
import com.github.mdc.stream.sql.build.StreamPipelineSqlBuilder;

public class StreamPipelineSqlBuilderTest extends StreamPipelineBaseTestCommon {
	String[] airlineheader = new String[]{"AirlineYear", "MonthOfYear", "DayofMonth", "DayOfWeek", "DepTime",
			"CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime",
			"CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut",
			"Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay"};
	String[] carrierheader = {"Code", "Description"};
	Logger log = Logger.getLogger(StreamPipelineSqlBuilderTest.class);

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumns() throws Exception {
		log.info("In testAllColumns() method Entry");
		String statement = "SELECT * FROM airline ";
		pipelineconfig.setLocal("true");
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String,Object>>> records = (List<List<Map<String,Object>>>) mdpsql.collect(true, null);
		for (List<Map<String,Object>> recs : records) {
			for(Object record: recs) {
				log.info(record);
			}
		}
		log.info("In testAllColumns() method Exit");		
	}
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsWithWhere() throws Exception {
		log.info("In testAllColumnsWithWhere() method Entry");
		String statement = "SELECT * FROM airline WHERE DayofMonth='8' and MonthOfYear='12'";
		pipelineconfig.setLocal("true");
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String,Object>>> records = (List<List<Map<String,Object>>>) mdpsql.collect(true, null);
		for (List<Map<String,Object>> recs : records) {
			for(Object record: recs) {
				log.info(record);
			}
		}
		log.info("In testAllColumnsWithWhere() method Exit");		
	}
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumns() throws Exception {
		log.info("In testRequiredColumns() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DepDelay FROM airline ";
		pipelineconfig.setLocal("true");
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String,Object>>> records = (List<List<Map<String,Object>>>) mdpsql.collect(true, null);
		for (List<Map<String,Object>> recs : records) {
			for(Object record: recs) {
				log.info(record);
			}
		}
		log.info("In testRequiredColumns() method Exit");		
	}
	
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhere() throws Exception {
		log.info("In testRequiredColumnsWithWhere() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DepDelay FROM airline WHERE DayofMonth='8' and MonthOfYear='12'";
		pipelineconfig.setLocal("true");
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String,Object>>> records = (List<List<Map<String,Object>>>) mdpsql.collect(true, null);
		for (List<Map<String,Object>> recs : records) {
			for(Object record: recs) {
				log.info(record);
			}
		}
		log.info("In testRequiredColumnsWithWhere() method Exit");		
	}
	
	
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereGreaterThan() throws Exception {
		log.info("In testRequiredColumnsWithWhereGreaterThan() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DayofMonth,MonthOfYear " + "FROM airline "
				+ "WHERE DayofMonth>8 and MonthOfYear>6";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(Long.valueOf((String) (String) rec.get("DayofMonth")) > 8);
				assertTrue(Long.valueOf((String) (String) rec.get("MonthOfYear")) > 6);
			}
		}
		log.info("In testRequiredColumnsWithWhereGreaterThan() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereLessThan() throws Exception {
		log.info("In testRequiredColumnsWithWhereLessThan() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DayofMonth,MonthOfYear " + "FROM airline "
				+ "WHERE DayofMonth<8 and MonthOfYear<6";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(Long.valueOf((String) (String) rec.get("DayofMonth")) < 8);
				assertTrue(Long.valueOf((String) (String) rec.get("MonthOfYear")) < 6);
			}
		}
		log.info("In testRequiredColumnsWithWhereLessThan() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereGreaterThanEquals() throws Exception {
		log.info("In testRequiredColumnsWithWhereGreaterThanEquals() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DayofMonth,MonthOfYear " + "FROM airline "
				+ "WHERE DayofMonth>=8 and MonthOfYear>=6";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(Long.valueOf((String) (String) rec.get("DayofMonth")) >= 8);
				assertTrue(Long.valueOf((String) (String) rec.get("MonthOfYear")) >= 6);
			}
		}
		log.info("In testRequiredColumnsWithWhereGreaterThanEquals() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereLessThanEquals() throws Exception {
		log.info("In testRequiredColumnsWithWhereLessThanEquals() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DayofMonth,MonthOfYear " + "FROM airline "
				+ "WHERE DayofMonth<=8 and MonthOfYear<=6";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(Long.valueOf((String) (String) rec.get("DayofMonth")) <= 8);
				assertTrue(Long.valueOf((String) (String) rec.get("MonthOfYear")) <= 6);
			}
		}
		log.info("In testRequiredColumnsWithWhereLessThanEquals() method Exit");
	}
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereLiteralFirst() throws Exception {
		log.info("In RequiredColumnsWithWhere() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DayofMonth,MonthOfYear " + "FROM airline "
				+ "WHERE '8'=DayofMonth and '12'=MonthOfYear";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				assertEquals("8", rec.get("DayofMonth"));
				assertEquals("12", rec.get("MonthOfYear"));
				log.info(rec);
			}
		}
		log.info("In RequiredColumnsWithWhere() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereColumnEquals() throws Exception {
		log.info("In testRequiredColumnsWithWhereColumnEquals() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DayofMonth,MonthOfYear " + "FROM airline "
				+ "WHERE DayofMonth=MonthOfYear";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				assertTrue(rec.get("DayofMonth").equals(rec.get("MonthOfYear")));
				log.info(rec);
			}
		}
		log.info("In testRequiredColumnsWithWhereColumnEquals() method Exit");
	}
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsCount() throws Exception {
		log.info("In testRequiredColumnsCount() method Entry");
		String statement = "SELECT count(*) FROM airline";				
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Integer>> records = (List<List<Integer>>) mdpsql.collect(true, null);
		
		assertEquals(Long.valueOf(46360), records.get(0).get(0));
		log.info("In testRequiredColumnsCount() method Exit");
	}
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsCountWithWhere() throws Exception {
		log.info("In testRequiredColumnsCountWithWhere() method Entry");
		String statement = "SELECT count(*) FROM airline WHERE DayofMonth=MonthOfYear";				
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Integer>> records = (List<List<Integer>>) mdpsql.collect(true, null);
		
		assertEquals(Long.valueOf(1522), records.get(0).get(0));
		log.info("In testRequiredColumnsCountWithWhere() method Exit");
	}
	
}
