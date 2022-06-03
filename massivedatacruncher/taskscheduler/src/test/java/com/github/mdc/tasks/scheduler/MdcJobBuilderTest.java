package com.github.mdc.tasks.scheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.github.mdc.common.DataCruncherContext;

public class MdcJobBuilderTest extends MassiveDataMRJobBase{

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
		assertEquals("/aircararrivaldelay",mdcjob.outputfolder);
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
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
		assertEquals("/aircararrivaldelay",mdcjob.outputfolder);
		List<DataCruncherContext> dccl = mdcjob.call();
		log.info(dccl);
		assertEquals(4, dccl.size());
	}
}
