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

import java.util.List;

import org.apache.log4j.Logger;
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

	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumns() throws Exception {
		log.info("In testFilterMDPSqlBuilderAirlines() method Entry");
		String statement = "SELECT * FROM airline ";
		pipelineconfig.setLocal("false");
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List> records = (List<List>) mdpsql.collect(true, null);
		for (List<List> recs : records) {
			for(Object record: recs) {
				log.info(record);
			}
		}
		log.info("In testFilterMDPSqlBuilderAirlines() method Exit");		
	}
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testAllColumnsWithWhere() throws Exception {
		log.info("In testFilterMDPSqlBuilderAirlines() method Entry");
		String statement = "SELECT * FROM airline WHERE DayofMonth='8' and MonthOfYear='12'";
		pipelineconfig.setLocal("false");
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List> records = (List<List>) mdpsql.collect(true, null);
		for (List<List> recs : records) {
			for(Object record: recs) {
				log.info(record);
			}
		}
		log.info("In testFilterMDPSqlBuilderAirlines() method Exit");		
	}
	
	@SuppressWarnings({"unchecked"})
	@Test
	public void testRequiredColumns() throws Exception {
		log.info("In testFilterMDPSqlBuilderAirlines() method Entry");
		String statement = "SELECT UniqueCarrier,ArrDelay,DepDelay FROM airline ";
		pipelineconfig.setLocal("false");
		StreamPipelineSql mdpsql = StreamPipelineSqlBuilder.newBuilder().add(airlinesamplesql, "airline", airlineheader, airsqltype)
				.setHdfs(hdfsfilepath)
				.setPipelineConfig(pipelineconfig).setSql(statement).build();
		List<List> records = (List<List>) mdpsql.collect(true, null);
		for (List<List> recs : records) {
			for(Object record: recs) {
				log.info(record);
			}
		}
		log.info("In testFilterMDPSqlBuilderAirlines() method Exit");		
	}
}
