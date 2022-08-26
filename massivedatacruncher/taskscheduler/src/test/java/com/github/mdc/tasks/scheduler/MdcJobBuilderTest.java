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
package com.github.mdc.tasks.scheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.github.mdc.common.DataCruncherContext;

public class MdcJobBuilderTest extends MassiveDataMRJobBase {

	@Test
	public void testMdcJobBuilder() {
		MapReduceApplication mdcjob = (MapReduceApplication) MapReduceApplicationBuilder.newBuilder()
				.addMapper(AirlineDataMapper.class, "/carriers")
				.addMapper(AirlineDataMapper.class, "/airlines")
				.addCombiner(AirlineDataMapper.class)
				.addReducer(AirlineDataMapper.class)
				.setOutputfolder("/aircararrivaldelay")
				.build();
		assertTrue(mdcjob.mappers.get(0).crunchmapper == AirlineDataMapper.class);
		assertEquals("/carriers", mdcjob.mappers.get(0).inputfolderpath);
		assertTrue(mdcjob.mappers.get(1).crunchmapper == AirlineDataMapper.class);
		assertEquals("/airlines", mdcjob.mappers.get(1).inputfolderpath);
		assertTrue(mdcjob.combiners.get(0) == AirlineDataMapper.class);
		assertTrue(mdcjob.reducers.get(0) == AirlineDataMapper.class);
		assertEquals("/aircararrivaldelay", mdcjob.outputfolder);
	}

	@SuppressWarnings({"rawtypes"})
	@Test
	public void testMdcJobCall() {
		MapReduceApplication mdcjob = (MapReduceApplication) MapReduceApplicationBuilder.newBuilder()
				.addMapper(AirlineDataMapper.class, airlines)
				.addCombiner(AirlineDataMapper.class)
				.addReducer(AirlineDataMapper.class)
				.setOutputfolder("/aircararrivaldelay")
				.build();
		assertTrue(mdcjob.mappers.get(0).crunchmapper == AirlineDataMapper.class);
		assertEquals(airlines, mdcjob.mappers.get(0).inputfolderpath);
		assertTrue(mdcjob.combiners.get(0) == AirlineDataMapper.class);
		assertTrue(mdcjob.reducers.get(0) == AirlineDataMapper.class);
		assertEquals("/aircararrivaldelay", mdcjob.outputfolder);
		List<DataCruncherContext> dccl = mdcjob.call();
		log.info(dccl);
		assertEquals(1, dccl.size());
	}
}
