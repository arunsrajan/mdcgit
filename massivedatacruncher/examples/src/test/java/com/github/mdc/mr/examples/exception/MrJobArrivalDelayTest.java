package com.github.mdc.mr.examples.exception;

import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.mr.examples.common.MassiveDataMRJobBaseException;
import com.github.mdc.tasks.scheduler.JobConfigrationBuilder;
import com.github.mdc.tasks.scheduler.JobConfiguration;
import com.github.mdc.tasks.scheduler.MdcJob;
import com.github.mdc.tasks.scheduler.MdcJobBuilder;
import com.github.mdc.tasks.scheduler.MdcJobYarn;

public class MrJobArrivalDelayTest extends MassiveDataMRJobBaseException{
	static String heapsize = "1024";
	static Logger log = Logger.getLogger(MrJobArrivalDelayTest.class);
	@Test
	public void airlinesCarrierJoinTest1() {
		JobConfiguration jc = JobConfigrationBuilder.newBuilder().setBlocksize("64").
		setBatchsize("2").
		setNumberofcontainers("1").
		setMinmem(heapsize).
		setMaxmem(heapsize).
		setNumofreducers("1").
		setGctype(MDCConstants.ZGC)
		.setIsblocksuserdefined("false").build();
		jc.setOutput(new Output(System.out));
		MdcJob mdcjob = (MdcJob) MdcJobBuilder.newBuilder()
				.addMapper(CarriersDataMapper.class, "/carriers")
				.addMapper(AirlineArrDelayDataMapper.class, "/1987")
				.addMapper(AirlineDepDelayDataMapper.class, "/1987")			
				.addCombiner(CarriersDataMapper.class)
				.addReducer(CarriersDataMapper.class)
				.setOutputfolder("/aircararrivaldelay")
				.setJobConf(jc)
				.build();
				
		List<DataCruncherContext> ctx = mdcjob.call();
		log.info(ctx);
	}
	@Test
	public void airlinesCarrierJoinTest2() {

		JobConfiguration jc = JobConfigrationBuilder.newBuilder().setBlocksize("64").
		setBatchsize("2").
		setNumberofcontainers("1").
		setMinmem(heapsize).
		setMaxmem(heapsize).
		setNumofreducers("3").
		setGctype(MDCConstants.ZGC)
		.setIsblocksuserdefined("false")
		.setExecmode(MDCConstants.EXECMODE_YARN).build();
		jc.setOutput(new Output(System.out));
		MdcJobYarn mdcjob = (MdcJobYarn) MdcJobBuilder.newBuilder()
				.addMapper(CarriersDataMapper.class, "/carriers")
				.addMapper(AirlineArrDelayDataMapper.class, "/1987")				
				.addCombiner(CarriersDataMapper.class)
				.addReducer(CarriersDataMapper.class)
				.setOutputfolder("/aircararrivaldelay")
				.setJobConf(jc)
				.build();
				
		List<DataCruncherContext> ctx = mdcjob.call();
	
	}
	@Test
	public void airlinesCarrierJoinTest3() {
		airlinesCarrierJoinTest1();
	}
	@Test
	public void airlinesCarrierJoinTest4() {
		airlinesCarrierJoinTest1();
	}
}
