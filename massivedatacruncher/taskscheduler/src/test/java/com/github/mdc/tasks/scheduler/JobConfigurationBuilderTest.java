package com.github.mdc.tasks.scheduler;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.tasks.scheduler.JobConfigrationBuilder;
import com.github.mdc.tasks.scheduler.JobConfiguration;

public class JobConfigurationBuilderTest extends TaskSchedulerCommon{

	@Test
	public void testJobConfigurationBuilder() {
		JobConfiguration jc = JobConfigrationBuilder.newBuilder().build();
		assertEquals("2",jc.getBatchsize());
		assertEquals("67108864",jc.getBlocksize());
		assertEquals(MDCConstants.GCCONFIG_DEFAULT,jc.getGctype());
		assertEquals(Boolean.valueOf("true"),jc.getHdfs());
		assertEquals("hdfs://127.0.0.1:9000",jc.getHdfsurl());
		assertEquals("false",jc.getIsblocksuserdefined());
		assertEquals("1024",jc.getMaxmem());
		assertEquals("1024",jc.getMinmem());
		assertEquals(null,jc.getMrjar());
		assertEquals(MDCConstants.NUMBEROFCONTAINERS_DEFAULT,jc.getNumberofcontainers());
		assertEquals("1",jc.getNumofreducers());
		assertEquals(null,jc.getOutput());
		assertEquals("1000",jc.getTepingdelay());
		assertEquals("127.0.0.1",jc.getTshost());
		assertEquals("1000",jc.getTsinitialdelay());
		assertEquals("1000",jc.getTspingdelay());
		assertEquals("11111", jc.getTsport());
		assertEquals("10000", jc.getTsrescheduledelay());
		assertEquals(null,jc.getTstempdir());
		assertEquals("127.0.0.1:2182", jc.getZkport());
		assertEquals("2000",jc.getZkretrydelay());
	}
	
}
