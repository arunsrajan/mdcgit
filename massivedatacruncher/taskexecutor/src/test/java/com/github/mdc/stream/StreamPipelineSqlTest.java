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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.junit.Test;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.stream.sql.StreamPipelineSql;
import com.github.mdc.stream.sql.StreamPipelineSqlBuilder;

public class StreamPipelineSqlTest extends StreamPipelineBaseTestCommon {
	String[] airlineheader = new String[]{"AirlineYear", "MonthOfYear", "DayofMonth", "DayOfWeek", "DepTime",
			"CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime",
			"CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut",
			"Cancelled", "CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay"};
	String[] carrierheader = {"Code", "Description"};
	Logger log = Logger.getLogger(StreamPipelineSqlTest.class);

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFilterMDPSqlBuilderAirlines() throws Exception {
		log.info("In testFilterMDPSqlBuilderAirlines() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DayofMonth,MonthOfYear " + "FROM airline "
				+ "WHERE DayofMonth='8' and MonthOfYear='12'";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				assertEquals("8", rec.get("DayofMonth"));
				assertEquals("12", rec.get("MonthOfYear"));
				log.info(rec);
			}
		}
		log.info("In testFilterMDPSqlBuilderAirlines() method Exit");
		pipelineconfig.setLocal("true");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFilterMDPSqlBuilderAirlinesLiteralGreaterThan() throws Exception {
		log.info("In testFilterMDPSqlBuilderAirlinesLiteralGreaterThan() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DayofMonth,MonthOfYear " + "FROM airline "
				+ "WHERE DayofMonth>8 and MonthOfYear>6";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(Long.valueOf((String) (String) rec.get("DayofMonth")) > 8);
				assertTrue(Long.valueOf((String) (String) rec.get("MonthOfYear")) > 6);
			}
		}
		log.info("In testFilterMDPSqlBuilderAirlinesLiteralGreaterThan() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFilterMDPSqlBuilderAirlinesLiteralLessThan() throws Exception {
		log.info("In testFilterMDPSqlBuilderAirlinesLiteralLessThan() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DayofMonth,MonthOfYear " + "FROM airline "
				+ "WHERE DayofMonth<8 and MonthOfYear<6";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(Long.valueOf((String) (String) rec.get("DayofMonth")) < 8);
				assertTrue(Long.valueOf((String) (String) rec.get("MonthOfYear")) < 6);
			}
		}
		log.info("In testFilterMDPSqlBuilderAirlinesLiteralLessThan() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFilterMDPSqlBuilderAirlinesLiteralGreaterThanEquals() throws Exception {
		log.info("In testFilterMDPSqlBuilderAirlinesLiteralGreaterThanEquals() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DayofMonth,MonthOfYear " + "FROM airline "
				+ "WHERE DayofMonth>=8 and MonthOfYear>=6";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(Long.valueOf((String) (String) rec.get("DayofMonth")) >= 8);
				assertTrue(Long.valueOf((String) (String) rec.get("MonthOfYear")) >= 6);
			}
		}
		log.info("In testFilterMDPSqlBuilderAirlinesLiteralGreaterThanEquals() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFilterMDPSqlBuilderAirlinesLiteralLessThanEquals() throws Exception {
		log.info("In testFilterMDPSqlBuilderAirlinesLiteralLessThanEquals() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DayofMonth,MonthOfYear " + "FROM airline "
				+ "WHERE DayofMonth<=8 and MonthOfYear<=6";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(Long.valueOf((String) (String) rec.get("DayofMonth")) <= 8);
				assertTrue(Long.valueOf((String) (String) rec.get("MonthOfYear")) <= 6);
			}
		}
		log.info("In testFilterMDPSqlBuilderAirlinesLiteralLessThanEquals() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFilterMDPSqlBuilderAirlinesLiteralGreaterThanEqualsOr() throws Exception {
		log.info("In testFilterMDPSqlBuilderAirlinesLiteralGreaterThanEqualsOr() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DayofMonth,MonthOfYear " + "FROM airline "
				+ "WHERE DayofMonth>=8 or MonthOfYear>=6";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(Long.valueOf((String) (String) rec.get("DayofMonth")) >= 8 || Long.valueOf((String) rec.get("MonthOfYear")) >= 6);
			}
		}
		log.info("In testFilterMDPSqlBuilderAirlinesLiteralGreaterThanEqualsOr() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFilterMDPSqlBuilderAirlinesLiteralLessThanEqualsOr() throws Exception {
		log.info("In testFilterMDPSqlBuilderAirlinesLiteralLessThanEqualsOr() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DayofMonth,MonthOfYear " + "FROM airline "
				+ "WHERE DayofMonth<=8 or MonthOfYear<=6";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(Long.valueOf((String) (String) rec.get("DayofMonth")) <= 8 || Long.valueOf((String) (String) rec.get("MonthOfYear")) <= 6);
			}
		}
		log.info("In testFilterMDPSqlBuilderAirlinesLiteralLessThanEqualsOr() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFilterMDPSqlBuilderAirlinesLiteralFirst() throws Exception {
		log.info("In testFilterMDPSqlBuilderAirlinesLiteralFirst() method Entry");
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
		log.info("In testFilterMDPSqlBuilderAirlinesLiteralFirst() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFilterMDPSqlBuilderAirlinesColumnEquals() throws Exception {
		log.info("In testFilterMDPSqlBuilderAirlinesColumnEquals() method Entry");
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
		log.info("In testFilterMDPSqlBuilderAirlinesColumnEquals() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderAirlinesCarriersJoin() throws Exception {
		log.info("In testMDPSqlBuilderAirlinesCarriersJoin() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DayofMonth,MonthOfYear,Description "
				+ "FROM airline join carriers on airline.UniqueCarrier = carriers.Code "
				+ "WHERE DayofMonth='8' and MonthOfYear='12'";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertEquals("8", rec.get("DayofMonth"));
				assertEquals("12", rec.get("MonthOfYear"));
			}
		}
		log.info("In testMDPSqlBuilderAirlinesCarriersJoin() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderAirlinesCarriersJoinCount() throws Exception {
		log.info("In testMDPSqlBuilderAirlinesCarriersJoinCount() method Entry");
		String statement = "SELECT UniqueCarrier,count(UniqueCarrier) "
				+ "FROM airline join carriers on airline.UniqueCarrier = carriers.Code "
				+ "WHERE DayofMonth='8' and MonthOfYear='12' group by UniqueCarrier";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		long totalrecords = 0;
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				totalrecords += (Long) rec.get("count()");
			}
		}
		assertEquals(132, totalrecords);
		log.info("In testMDPSqlBuilderAirlinesCarriersJoinCount() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderAirlinesCarriersJoinRecordCount() throws Exception {
		log.info("In testMDPSqlBuilderAirlinesCarriersJoinRecordCount() method Entry");
		String statement = "SELECT UniqueCarrier,Code "
				+ "FROM airline join carriers on airline.UniqueCarrier = carriers.Code "
				+ "WHERE DayofMonth='8' and MonthOfYear='12'";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		long totalrecords = 0;
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			totalrecords += recs.size();
			for (Map<String, Object> rec : recs) {
				log.info(rec);
			}
		}
		assertEquals(132, totalrecords);
		log.info("In testMDPSqlBuilderAirlinesCarriersJoinRecordCount() method Exit");
	}


	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderAirlinesCarriersLeftJoinRecordCount() throws Exception {
		log.info("In testMDPSqlBuilderAirlinesCarriersLeftJoinRecordCount() method Entry");
		String statement = "SELECT UniqueCarrier,Code "
				+ "FROM carriers left join airline on airline.UniqueCarrier = carriers.Code "
				+ "WHERE airline.DayofMonth='8' and airline.MonthOfYear='12'";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
			}
		}
		log.info("In testMDPSqlBuilderAirlinesCarriersLeftJoinRecordCount() method Exit");
	}


	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderAirlinesCarrierAirpJoin() throws Exception {
		log.info("In testMDPSqlBuilderAirlinesCarrierAirpJoin() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DayofMonth,MonthOfYear,Description,Origin,airport "
				+ "FROM airline join carriers on airline.UniqueCarrier = carriers.Code "
				+ " join airports on airports.iata = airline.Origin " + "WHERE DayofMonth='8' and MonthOfYear='12'";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype)
				.add(airportssample, "airports", airportsheader, airportstype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertEquals("8", rec.get("DayofMonth"));
				assertEquals("12", rec.get("MonthOfYear"));
				assertNotNull(rec.get("airport"));
			}
		}
		log.info("In testMDPSqlBuilderAirlinesCarrierAirpJoin() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderAirlinesCarrierJoinCarrierSpecific() throws Exception {
		log.info("In testMDPSqlBuilderAirlinesCarrierJoinCarrierSpecific() method Entry");
		String statement = "SELECT ArrDelay,DepDelay,DayofMonth,MonthOfYear,Code,Description "
				+ "FROM airline join carriers on airline.UniqueCarrier = carriers.Code "
				+ "WHERE DayofMonth='8' and MonthOfYear='8' and Code='AQ'";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertEquals("8", rec.get("DayofMonth"));
				assertEquals("8", rec.get("MonthOfYear"));
				assertEquals("AQ", rec.get("Code"));
			}
		}
		log.info("In testMDPSqlBuilderAirlinesCarrierJoinCarrierSpecific() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderAirlinesCarriersJoinOr() throws Exception {
		log.info("In testMDPSqlBuilderAirlinesCarriersJoinOr() method Entry");
		String statement = "SELECT ArrDelay,DepDelay,DayofMonth,MonthOfYear,Code,Description "
				+ "FROM airline join carriers on airline.UniqueCarrier = carriers.Code "
				+ "WHERE DayofMonth='8' and MonthOfYear='8' or Code='A'";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertEquals("8", rec.get("DayofMonth"));
				assertEquals("8", rec.get("MonthOfYear"));
				assertNotNull(rec.get("Code"));
			}
		}
		log.info("In testMDPSqlBuilderAirlinesCarriersJoinOr() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testJoinMDPSqlBuilderAirlinesCarrierLeftJoin() throws Exception {
		log.info("In testJoinMDPSqlBuilderAirlinesCarrierLeftJoin() method Entry");
		String statement = "SELECT ArrDelay,DepDelay,DayofMonth,MonthOfYear,Code,Description "
				+ "FROM airline left join carriers on airline.UniqueCarrier = carriers.Code " + "WHERE DayofMonth='8'";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertEquals("8", rec.get("DayofMonth"));
			}
		}
		log.info("In testJoinMDPSqlBuilderAirlinesCarrierLeftJoin() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderCarriersAllColumns() throws Exception {
		log.info("In testMDPSqlBuilderCarriersAllColumns() method Entry");
		String statement = "SELECT * " + "FROM carriers";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(rec instanceof Map);
				assertNotNull(rec.get("Code"));
				assertNotNull(rec.get("Description"));
			}
		}
		log.info("In testMDPSqlBuilderCarriersAllColumns() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderAirlinesAllColumns() throws Exception {
		log.info("In testMDPSqlBuilderAirlinesAllColumns() method Entry");
		String statement = "SELECT * " + "FROM airline";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				assertTrue(rec instanceof Map);
			}
		}
		log.info("In testMDPSqlBuilderAirlinesAllColumns() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderCarriersSumArrDelay() throws Exception {
		log.info("In testMDPSqlBuilderCarriersSumArrDelay() method Entry");
		String statement = "SELECT UniqueCarrier,AirlineYear,sum(ArrDelay) "
				+ "FROM airline where ArrDelay<>'NA' group by UniqueCarrier,AirlineYear";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sum += (Long) rec.get("sum(ArrDelay)");
			}
		}
		assertEquals(-63278, sum);
		log.info("In testMDPSqlBuilderCarriersSumArrDelay() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderCarriersSumArrDelayCountArrivalDelay() throws Exception {
		log.info("In testMDPSqlBuilderCarriersSumArrDelayCountArrivalDelay() method Entry");
		String statement = "SELECT UniqueCarrier,AirlineYear,sum(ArrDelay),count(ArrDelay) "
				+ "FROM airline where ArrDelay<>'NA' group by UniqueCarrier,AirlineYear";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec.get("UniqueCarrier"));
				log.info(rec.get("AirlineYear"));
				log.info(rec.get("sum(ArrDelay)"));
				log.info(rec.get("count()"));
				sum += (Long) rec.get("sum(ArrDelay)");
			}
		}
		assertEquals(-63278, sum);
		log.info("In testMDPSqlBuilderCarriersSumArrDelayCountArrivalDelay() method Exit");
	}


	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderCarriersSumArrDelayCountArrivalDelaySwap() throws Exception {
		log.info("In testMDPSqlBuilderCarriersSumArrDelayCountArrivalDelaySwap() method Entry");
		String statement = "SELECT UniqueCarrier,AirlineYear,count(ArrDelay),sum(ArrDelay) "
				+ "FROM airline where ArrDelay<>'NA' group by UniqueCarrier,AirlineYear";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec.get("UniqueCarrier"));
				log.info(rec.get("AirlineYear"));
				log.info(rec.get("sum(ArrDelay)"));
				log.info(rec.get("count()"));
				sum += (Long) rec.get("sum(ArrDelay)");
			}
		}
		assertEquals(-63278, sum);
		log.info("In testMDPSqlBuilderCarriersSumArrDelayCountArrivalDelaySwap() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderCarriersSumArrDelayFirstColumn() throws Exception {
		log.info("In testMDPSqlBuilderCarriersSumArrDelay() method Entry");
		String statement = "SELECT sum(ArrDelay),UniqueCarrier,AirlineYear "
				+ "FROM airline where ArrDelay<>'NA' group by UniqueCarrier,AirlineYear";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sum += (Long) rec.get("sum(ArrDelay)");
			}
		}
		assertEquals(-63278, sum);
		log.info("In testMDPSqlBuilderCarriersSumArrDelay() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderCarriersSumArrDelayFirstColumnCountSecondColumn() throws Exception {
		log.info("In testMDPSqlBuilderCarriersSumArrDelayFirstColumnCountSecondColumn() method Entry");
		String statement = "SELECT sum(ArrDelay),count(ArrDelay),UniqueCarrier,AirlineYear "
				+ "FROM airline where ArrDelay<>'NA' group by UniqueCarrier,AirlineYear";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sum += (Long) rec.get("sum(ArrDelay)");
			}
		}
		assertEquals(-63278, sum);
		log.info("In testMDPSqlBuilderCarriersSumArrDelayFirstColumnCountSecondColumn() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderCarriersSumArrDelayFirstColumnCountSecondColumnSwap() throws Exception {
		log.info("In testMDPSqlBuilderCarriersSumArrDelayFirstColumnCountSecondColumnSwap() method Entry");
		String statement = "SELECT count(ArrDelay),sum(ArrDelay),UniqueCarrier,AirlineYear "
				+ "FROM airline where ArrDelay<>'NA' group by UniqueCarrier,AirlineYear";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sum += (Long) rec.get("sum(ArrDelay)");
			}
		}
		assertEquals(-63278, sum);
		log.info("In testMDPSqlBuilderCarriersSumArrDelayFirstColumnCountSecondColumnSwap() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderArrDelaySumArrDelayCountMid() throws Exception {
		log.info("In testMDPSqlBuilderArrDelaySumArrDelayCountMid() method Entry");
		String statement = "SELECT UniqueCarrier,count(ArrDelay),sum(ArrDelay),AirlineYear "
				+ "FROM airline where ArrDelay<>'NA' group by UniqueCarrier,AirlineYear";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sum += (Long) rec.get("sum(ArrDelay)");
			}
		}
		assertEquals(-63278, sum);
		log.info("In testMDPSqlBuilderArrDelaySumArrDelayCountMid() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderArrDelaySumArrDelayCountMidSwap() throws Exception {
		pipelineconfig.setBlocksize("1");
		pipelineconfig.setIsblocksuserdefined("true");
		log.info("In testMDPSqlBuilderArrDelaySumArrDelayCountMidSwap() method Entry");
		String statement = "SELECT UniqueCarrier,sum(ArrDelay),count(ArrDelay),AirlineYear "
				+ "FROM airline where ArrDelay<>'NA' group by UniqueCarrier,AirlineYear";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sumarrdelay = 0, reccount = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sumarrdelay += (Long) rec.get("sum(ArrDelay)");
				reccount += (Long) rec.get("count()");
			}
		}
		assertEquals(-63278, sumarrdelay);
		assertEquals(45957, reccount);
		pipelineconfig.setIsblocksuserdefined("false");
		pipelineconfig.setBlocksize("20");
		log.info("In testMDPSqlBuilderArrDelaySumArrDelayCountMidSwap() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderArrDelaySumArrDelayCountMonthMidSwap() throws Exception {
		pipelineconfig.setBlocksize("1");
		pipelineconfig.setIsblocksuserdefined("true");
		log.info("In testMDPSqlBuilderArrDelaySumArrDelayCountMonthMidSwap() method Entry");
		String statement = "SELECT UniqueCarrier,sum(ArrDelay),count(ArrDelay),AirlineYear,MonthOfYear "
				+ "FROM airline where ArrDelay<>'NA' group by UniqueCarrier,AirlineYear,MonthOfYear";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sumarrdelay = 0, reccount = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sumarrdelay += (Long) rec.get("sum(ArrDelay)");
				reccount += (Long) rec.get("count()");
			}
		}
		assertEquals(-63278, sumarrdelay);
		assertEquals(45957, reccount);
		pipelineconfig.setIsblocksuserdefined("false");
		pipelineconfig.setBlocksize("20");
		log.info("In testMDPSqlBuilderArrDelaySumArrDelayCountMonthMidSwap() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderDepDelaySumDepDelayCountMidSwap() throws Exception {
		log.info("In testMDPSqlBuilderDepDelaySumDepDelayCountMidSwap() method Entry");
		String statement = "SELECT UniqueCarrier,sum(DepDelay),count(DepDelay),AirlineYear "
				+ "FROM airline where DepDelay<>'NA' group by UniqueCarrier,AirlineYear";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sum += (Long) rec.get("sum(DepDelay)");
			}
		}
		assertEquals(20168, sum);
		log.info("In testMDPSqlBuilderDepDelaySumDepDelayCountMidSwap() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderCarriersSumDepDelay() throws Exception {
		log.info("In testMDPSqlBuilderCarriersSumDepDelay() method Entry");
		String statement = "SELECT UniqueCarrier,AirlineYear,sum(DepDelay) "
				+ "FROM airline where DepDelay<>'NA' group by UniqueCarrier,AirlineYear";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(new PipelineConfig()).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sum += (Long) rec.get("sum(DepDelay)");
			}
		}
		assertEquals(20168, sum);
		log.info("In testMDPSqlBuilderCarriersSumDepDelay() method Exit");
	}


	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderGroupByCarriersOnlySumDepDelay() throws Exception {
		pipelineconfig.setLocal("true");
		log.info("In testMDPSqlBuilderGroupByCarriersOnlySumDepDelay() method Entry");
		String statement = "SELECT UniqueCarrier,sum(DepDelay) "
				+ "FROM airline where DepDelay<>'DepDelay' and DepDelay<>'NA' group by UniqueCarrier";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sum += (Long) rec.get("sum(DepDelay)");
			}
		}
		assertEquals(20168, sum);
		log.info("In testMDPSqlBuilderGroupByCarriersOnlySumDepDelay() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderGroupByCarriersOnlyArrivalDelay() throws Exception {
		pipelineconfig.setLocal("true");
		log.info("In testMDPSqlBuilderGroupByCarriersOnlyArrivalDelay() method Entry");
		String statement = "SELECT UniqueCarrier,sum(ArrDelay) "
				+ "FROM airline where ArrDelay<>'ArrDelay' and ArrDelay<>'NA' group by UniqueCarrier";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sum += (Long) rec.get("sum(ArrDelay)");
			}
		}
		assertEquals(-63278, sum);
		log.info("In testMDPSqlBuilderGroupByCarriersOnlyArrivalDelay() method Exit");
	}


	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderCountArrivalDelay() throws Exception {
		pipelineconfig.setLocal("true");
		log.info("In testMDPSqlBuilderCountArrivalDelay() method Entry");
		String statement = "SELECT count(ArrDelay) "
				+ "FROM airline where ArrDelay<>'ArrDelay' and ArrDelay<>'NA'";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
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
		assertEquals(45957, sum);
		log.info("In testMDPSqlBuilderCountArrivalDelay() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderCountArrDelaySumArrDelayBiggerFiles() throws Exception {
		pipelineconfig.setLocal("true");
		log.info("In testMDPSqlBuilderCountArrDelaySumArrDelayBiggerFiles() method Entry");
		String statement = "SELECT UniqueCarrier,count(ArrDelay),sum(ArrDelay) "
				+ "FROM airline where ArrDelay<>'ArrDelay' and ArrDelay<>'NA' group by UniqueCarrier";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.add(carriers, "carriers", carrierheader, carriersqltype).setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sum += (Long) rec.get("sum(ArrDelay)");
			}
		}
		assertEquals(-63278, sum);
		log.info("In testMDPSqlBuilderCountArrDelaySumArrDelayBiggerFiles() method Exit");
	}


	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderUniqueCarrierCountArrDelay() throws Exception {
		pipelineconfig.setLocal("true");
		log.info("In testMDPSqlBuilderUniqueCarrierCountArrDelay() method Entry");
		String statement = "SELECT UniqueCarrier,count(ArrDelay) "
				+ "FROM airline where ArrDelay<>'ArrDelay' and ArrDelay<>'NA' group by UniqueCarrier";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sum += (Long) rec.get("count()");
			}
		}
		assertEquals(45957, sum);
		log.info("In testMDPSqlBuilderUniqueCarrierCountArrDelay() method Exit");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMDPSqlBuilderUniqueCarrierCountArrDelaySA() throws Exception {
		PipelineConfig pc = new PipelineConfig();
		pc.setLocal("false");
		log.info("In testMDPSqlBuilderUniqueCarrierCountArrDelaySA() method Entry");
		String statement = "SELECT UniqueCarrier,count(ArrDelay) "
				+ "FROM airline where ArrDelay<>'ArrDelay' and ArrDelay<>'NA' group by UniqueCarrier";
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pc).setSql(statement).build();
		List<List<Map<String, Object>>> records = (List<List<Map<String, Object>>>) mdpsql.collect(true, null);
		long sum = 0;
		for (List<Map<String, Object>> recs : records) {
			for (Map<String, Object> rec : recs) {
				log.info(rec);
				sum += (Long) rec.get("count()");
			}
		}
		assertEquals(45957, sum);
		log.info("In testMDPSqlBuilderUniqueCarrierCountArrDelaySA() method Exit");
	}
}
