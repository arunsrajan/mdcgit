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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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

	@BeforeClass
	public static void pipelineSetup() {
		pipelineconfig.setLocal("false");
	}
	
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumns() throws Exception {
		log.info("In testAllColumns() method Entry");
		String statement = "SELECT * FROM airline ";
		
		int total = 0;
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String,Object>>> records = (List<List<Map<String,Object>>>) mdpsql.collect(true, null);
		for (List<Map<String,Object>> recs : records) {
			for(Map<String,Object> record: recs) {
				total++;
				assertTrue(record.keySet().size() == 29);
				log.info(record);
			}
		}
		assertEquals(46360, total);
		
		log.info("In testAllColumns() method Exit");		
	}
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsWithWhere() throws Exception {
		log.info("In testAllColumnsWithWhere() method Entry");
		String statement = "SELECT * FROM airline WHERE airline.DayofMonth='8' and airline.MonthOfYear='12'";
		
		int total = 0;
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String,Object>>> records = (List<List<Map<String,Object>>>) mdpsql.collect(true, null);
		for (List<Map<String,Object>> recs : records) {
			for(Map<String,Object> record: recs) {
				total++;
				assertTrue(record.keySet().size() == 29);
				assertTrue(record.get("DayofMonth").equals("8") && record.get("MonthOfYear").equals("12"));
				log.info(record);
			}
		}
		assertEquals(132,total);
		
		log.info("In testAllColumnsWithWhere() method Exit");		
	}
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumns() throws Exception {
		log.info("In testRequiredColumns() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DepDelay FROM airline ";
		
		int total = 0;
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String,Object>>> records = (List<List<Map<String,Object>>>) mdpsql.collect(true, null);
		for (List<Map<String,Object>> recs : records) {
			for(Map<String,Object> record: recs) {
				total++;
				assertTrue(record.keySet().size() == 3);
				assertTrue(record.keySet().size() == 3);
				assertTrue(record.containsKey("UniqueCarrier"));
				assertTrue(record.containsKey("ArrDelay"));
				assertTrue(record.containsKey("DepDelay"));
				log.info(record);
			}
		}
		assertEquals(46360,total);
		
		log.info("In testRequiredColumns() method Exit");		
	}
	
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhere() throws Exception {
		log.info("In testRequiredColumnsWithWhere() method Entry");
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DepDelay FROM airline WHERE airline.DayofMonth='8' and airline.MonthOfYear='12'";
		
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String,Object>>> records = (List<List<Map<String,Object>>>) mdpsql.collect(true, null);
		int total = 0 ;
		for (List<Map<String,Object>> recs : records) {
			for(Map<String,Object> record: recs) {
				total++;
				assertTrue(record.keySet().size() == 3);
				assertTrue(record.containsKey("UniqueCarrier"));
				assertTrue(record.containsKey("ArrDelay"));
				assertTrue(record.containsKey("DepDelay"));
				log.info(record);
			}
		}
		assertEquals(132,total);
		
		log.info("In testRequiredColumnsWithWhere() method Exit");		
	}
	
	
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsWithWhereGreaterThan() throws Exception {
		log.info("In testRequiredColumnsWithWhereGreaterThan() method Entry");
		
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear " + "FROM airline "
				+ "WHERE airline.DayofMonth>8 and airline.MonthOfYear>6";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(rec.keySet().size() == 4);
				assertTrue(rec.containsKey("UniqueCarrier"));
				assertTrue(rec.containsKey("ArrDelay"));
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
		
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear " + "FROM airline "
				+ "WHERE airline.DayofMonth<8 and airline.MonthOfYear<6";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(rec.keySet().size() == 4);
				assertTrue(rec.containsKey("UniqueCarrier"));
				assertTrue(rec.containsKey("ArrDelay"));
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
		
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear " + "FROM airline "
				+ "WHERE airline.DayofMonth>=8 and airline.MonthOfYear>=6";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(rec.keySet().size() == 4);
				assertTrue(rec.containsKey("UniqueCarrier"));
				assertTrue(rec.containsKey("ArrDelay"));
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
		
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear " + "FROM airline "
				+ "WHERE airline.DayofMonth<=8 and airline.MonthOfYear<=6";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(rec.keySet().size() == 4);
				assertTrue(rec.containsKey("UniqueCarrier"));
				assertTrue(rec.containsKey("ArrDelay"));
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
		
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear " + "FROM airline "
				+ "WHERE '8'=airline.DayofMonth and '12'=airline.MonthOfYear";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				assertTrue(rec.keySet().size() == 4);
				assertTrue(rec.containsKey("UniqueCarrier"));
				assertTrue(rec.containsKey("ArrDelay"));
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
		
		String statement = "SELECT airline.UniqueCarrier,airline.ArrDelay,airline.DayofMonth,airline.MonthOfYear " + "FROM airline "
				+ "WHERE airline.DayofMonth=airline.MonthOfYear";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				assertTrue(rec.keySet().size() == 4);
				assertTrue(rec.containsKey("UniqueCarrier"));
				assertTrue(rec.containsKey("ArrDelay"));
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
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Integer>> records = (List<List<Integer>>) mdpsql.collect(true, null);		
		assertEquals(Long.valueOf(46360), records.get(0).get(0));
		
		log.info("In testRequiredColumnsCount() method Exit");
	}
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsCountWithWhere() throws Exception {
		log.info("In testRequiredColumnsCountWithWhere() method Entry");
		
		String statement = "SELECT count(*) FROM airline WHERE airline.DayofMonth=airline.MonthOfYear";				
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Integer>> records = (List<List<Integer>>) mdpsql.collect(true, null);
		
		assertEquals(Long.valueOf(1522), records.get(0).get(0));
		
		log.info("In testRequiredColumnsCountWithWhere() method Exit");
	}
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsSumWithWhere() throws Exception {
		log.info("In testAllColumnsSumWithWhere() method Entry");
		
		String statement = "SELECT sum(airline.ArrDelay) FROM airline WHERE '8'=airline.DayofMonth and '12'=airline.MonthOfYear and airline.ArrDelay <> 'NA' and airline.ArrDelay <> 'ArrDelay'";				
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Integer>> records = (List<List<Integer>>) mdpsql.collect(true, null);
		
		assertEquals(Integer.valueOf(-362), records.get(0).get(0));
		
		log.info("In testAllColumnsSumWithWhere() method Exit");
	}
	
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsMinWithWhere() throws Exception {
		log.info("In testAllColumnsMinWithWhere() method Entry");
		
		String statement = "SELECT min(airline.ArrDelay) FROM airline WHERE '8'=airline.DayofMonth and '12'=airline.MonthOfYear and airline.ArrDelay <> 'NA' and airline.ArrDelay <> 'ArrDelay'";				
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Integer>> records = (List<List<Integer>>) mdpsql.collect(true, null);		
		assertEquals(Integer.valueOf(-27), records.get(0).get(0));
		
		log.info("In testAllColumnsMinWithWhere() method Exit");
	}
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsMaxWithWhere() throws Exception {
		log.info("In testAllColumnsMaxWithWhere() method Entry");
		
		String statement = "SELECT max(airline.ArrDelay) FROM airline WHERE '8'=airline.DayofMonth and '12'=airline.MonthOfYear and airline.ArrDelay <> 'NA' and airline.ArrDelay <> 'ArrDelay'";				
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Integer>> records = (List<List<Integer>>) mdpsql.collect(true, null);
		assertEquals(Integer.valueOf(44), records.get(0).get(0));
		
		log.info("In testAllColumnsMaxWithWhere() method Exit");
	}
	
	
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumnsJoin() throws Exception {
		log.info("In testRequiredColumnsJoin() method Entry");
		
		String statement = "SELECT airline.DayofMonth,airline.MonthOfYear,airline.UniqueCarrier,carriers.Code "
				+ "FROM airline inner join carriers on airline.UniqueCarrier = carriers.Code WHERE '8' = airline.DayofMonth and '12'= airline.MonthOfYear";				
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		long totalrecords = 0;
		List<List<Map<String,Object>>> records = (List<List<Map<String,Object>>>) mdpsql.collect(true, null);
		for (List<Map<String,Object>> recs : records) {
			totalrecords += recs.size();
			for (Map<String,Object> rec : recs) {
				log.info(rec);
				assertTrue(rec.keySet().size() == 4);
				assertTrue(rec.containsKey("UniqueCarrier"));
				assertTrue(rec.containsKey("Code"));
				assertEquals("8", rec.get("DayofMonth"));
				assertEquals("12", rec.get("MonthOfYear"));
			}
		}
		assertEquals(132, totalrecords);
		
		log.info("In testRequiredColumnsJoin() method Exit");
	}
	
	@Test
	public void testRequiredColumnsJoinCarrierSpecific() throws Exception {
		log.info("In testRequiredColumnsJoinCarrierSpecific() method Entry");
		
		String statement = "SELECT airline.ArrDelay,airline.DepDelay,airline.DayofMonth,airline.MonthOfYear,carriers.Code,carriers.Description "
				+ "FROM airline join carriers on airline.UniqueCarrier = carriers.Code "
				+ "WHERE airline.DayofMonth='8' and airline.MonthOfYear='8' and carriers.Code='AQ'";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String,Object>>> records = (List<List<Map<String,Object>>>) mdpsql.collect(true, null);
		for (List<Map<String,Object>> recs : records) {
			for (Map<String,Object> rec : recs) {
				log.info(rec);
				assertTrue(rec.keySet().size() == 6);
				assertTrue(rec.containsKey("DepDelay"));
				assertTrue(rec.containsKey("ArrDelay"));
				assertTrue(rec.containsKey("Code"));
				assertTrue(rec.containsKey("Description"));
				assertEquals("8", rec.get("DayofMonth"));
				assertEquals("8", rec.get("MonthOfYear"));
			}
		}
		
		log.info("In testRequiredColumnsJoinCarrierSpecific() method Exit");
	}
	
	@Test
	public void testCountAllColumnsWithWhereAndJoin() throws Exception {
		log.info("In testCountAllColumnsWithWhereAndJoin() method Entry");
		
		String statement = "SELECT count(*) "
				+ "FROM airline join carriers on airline.UniqueCarrier = carriers.Code "
				+ "WHERE airline.DayofMonth='8' and airline.MonthOfYear='8' and carriers.Code='AQ' and carriers.Code<>'Code'";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Long>> records = (List<List<Long>>) mdpsql.collect(true, null);
		int total = 0;
		for (List<Long> recs : records) {
			for (Long rec : recs) {
				total += rec;
			}
		}
		assertEquals(131, total);
		
		log.info("In testCountAllColumnsWithWhereAndJoin() method Exit");
	}
	
	
	@Test
	public void testPrintAllColumnsCountWithWhereAndJoin() throws Exception {
		log.info("In testPrintAllColumnsCountWithWhereAndJoin() method Entry");
		
		String statement = "SELECT * "
				+ "FROM airline join carriers on airline.UniqueCarrier = carriers.Code "
				+ "WHERE airline.DayofMonth='8' and airline.MonthOfYear='8' and carriers.Code='AQ' and carriers.Code<>'Code'";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String,Object>>> records = (List<List<Map<String,Object>>>) mdpsql.collect(true, null);
		int total = 0;
		for (List<Map<String,Object>> recs : records) {
			for (Map<String,Object> rec : recs) {
				total++;
				assertTrue(rec.keySet().size() == 31);
				assertTrue(rec.get("DayofMonth").equals("8"));
				assertTrue(rec.get("MonthOfYear").equals("8"));
				assertTrue(rec.get("Code").equals("AQ"));
				log.info(rec);
			}
		}
		assertEquals(131, total);
		
		log.info("In testPrintAllColumnsCountWithWhereAndJoin() method Exit");
	}
	
	@AfterClass
	public static void pipelineConfigReset() {
		pipelineconfig.setLocal("true");
	}
	
}
