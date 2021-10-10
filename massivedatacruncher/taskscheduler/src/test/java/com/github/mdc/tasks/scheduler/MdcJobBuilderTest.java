package com.github.mdc.tasks.scheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.tasks.scheduler.MapReduceApplicatiion;
import com.github.mdc.tasks.scheduler.MapReduceApplicationBuilder;

public class MdcJobBuilderTest extends MassiveDataMRJobBase{

	@Test
	public void testMdcJobBuilder() {
		MapReduceApplicatiion mdcjob = (MapReduceApplicatiion) MapReduceApplicationBuilder.newBuilder()
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
		MapReduceApplicatiion mdcjob = (MapReduceApplicatiion) MapReduceApplicationBuilder.newBuilder()
				.addMapper(AirlineDataMapper.class, "/airlinesample")				
				.addCombiner(AirlineDataMapper.class)
				.addReducer(AirlineDataMapper.class)
				.setOutputfolder("/aircararrivaldelay")
				.build();
		assertTrue(mdcjob.mappers.get(0).crunchmapper == AirlineDataMapper.class);
		assertEquals("/airlinesample", mdcjob.mappers.get(0).inputfolderpath);
		assertTrue(mdcjob.combiners.get(0) == AirlineDataMapper.class);
		assertTrue(mdcjob.reducers.get(0) == AirlineDataMapper.class);
		assertEquals("/aircararrivaldelay",mdcjob.outputfolder);
		List<DataCruncherContext> dccl = mdcjob.call();
		assertEquals(-63278l, dccl.get(0).get("AQ").iterator().next());
	}
}
