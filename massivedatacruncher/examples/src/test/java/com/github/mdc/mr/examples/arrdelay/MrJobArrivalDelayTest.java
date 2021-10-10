package com.github.mdc.mr.examples.arrdelay;

import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.mr.examples.common.MassiveDataMRJobBase;
import com.github.mdc.mr.examples.join.AirlineArrDelayDataMapper;
import com.github.mdc.mr.examples.join.AirlineDepDelayDataMapper;
import com.github.mdc.mr.examples.join.CarriersDataMapper;
import com.github.mdc.tasks.scheduler.JobConfigurationBuilder;
import com.github.mdc.tasks.scheduler.JobConfiguration;
import com.github.mdc.tasks.scheduler.MapReduceApplicatiion;
import com.github.mdc.tasks.scheduler.MapReduceApplicationBuilder;
import com.github.mdc.tasks.scheduler.MapReduceApplicationIgnite;
import com.github.mdc.tasks.scheduler.MapReduceApplicationYarn;

public class MrJobArrivalDelayTest extends MassiveDataMRJobBase{
	static String heapsize = "2048";
	static Logger log = Logger.getLogger(MrJobArrivalDelayTest.class);
	@Test
	public void airlinesCarrierJoinUserDefined128() {
		JobConfiguration jc = JobConfigurationBuilder.newBuilder().setBlocksize("128").
		setBatchsize("1").
		setNumberofcontainers("1").
		setMinmem(heapsize).
		setMaxmem(heapsize).
		setNumofreducers("1").
		setGctype(MDCConstants.ZGC)
		.setIsblocksuserdefined("true").build();
		jc.setOutput(new Output(System.out));
		MapReduceApplicatiion mdcjob = (MapReduceApplicatiion) MapReduceApplicationBuilder.newBuilder()
				.addMapper(CarriersDataMapper.class, "/carriers")
				.addMapper(AirlineArrDelayDataMapper.class, "/airline1989")
				.addMapper(AirlineDepDelayDataMapper.class, "/airline1989")			
				.addCombiner(CarriersDataMapper.class)
				.addReducer(CarriersDataMapper.class)
				.setOutputfolder("/aircararrivaldelay")
				.setJobConf(jc)
				.build();
				
		List<DataCruncherContext> ctx = mdcjob.call();
		log.info(ctx);
	}
	
	@Test
	public void airlinesCarrierJoinUserDefined64() {
		JobConfiguration jc = JobConfigurationBuilder.newBuilder().setBlocksize("64").
		setBatchsize("1").
		setNumberofcontainers("1").
		setMinmem(heapsize).
		setMaxmem(heapsize).
		setNumofreducers("1").
		setGctype(MDCConstants.ZGC)
		.setIsblocksuserdefined("true").build();
		jc.setOutput(new Output(System.out));
		MapReduceApplicatiion mdcjob = (MapReduceApplicatiion) MapReduceApplicationBuilder.newBuilder()
				.addMapper(CarriersDataMapper.class, "/carriers")
				.addMapper(AirlineArrDelayDataMapper.class, "/airline1989")
				.addMapper(AirlineDepDelayDataMapper.class, "/airline1989")			
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

		JobConfiguration jc = JobConfigurationBuilder.newBuilder().setBlocksize("64").
		setBatchsize("1").
		setNumberofcontainers("1").
		setMinmem(heapsize).
		setMaxmem(heapsize).
		setNumofreducers("1").
		setGctype(MDCConstants.ZGC)
		.setIsblocksuserdefined("false")
		.setExecmode(MDCConstants.EXECMODE_YARN).build();
		jc.setOutput(new Output(System.out));
		MapReduceApplicationYarn mdcjob = (MapReduceApplicationYarn) MapReduceApplicationBuilder.newBuilder()
				.addMapper(CarriersDataMapper.class, "/carriers")
				.addMapper(AirlineArrDelayDataMapper.class, "/airline1989")
				.addMapper(AirlineDepDelayDataMapper.class, "/airline1989")
				.addCombiner(CarriersDataMapper.class)
				.addReducer(CarriersDataMapper.class)
				.setOutputfolder("/aircararrivaldelay")
				.setJobConf(jc)
				.build();
				
		List<DataCruncherContext> ctx = mdcjob.call();
	
	}
	@Test
	public void airlinesCarrierJoinTest3() {

		JobConfiguration jc = JobConfigurationBuilder.newBuilder().setBlocksize("64").
		setBatchsize("2").
		setNumberofcontainers("1").
		setMinmem(heapsize).
		setMaxmem(heapsize).
		setNumofreducers("3").
		setGctype(MDCConstants.ZGC)
		.setIsblocksuserdefined("true")
		.setExecmode(MDCConstants.EXECMODE_IGNITE).build();
		jc.setOutput(new Output(System.out));
		MapReduceApplicationIgnite mdcjob = (MapReduceApplicationIgnite) MapReduceApplicationBuilder.newBuilder()
				.addMapper(CarriersDataMapper.class, "/carriers")
				.addMapper(AirlineArrDelayDataMapper.class, "/airlines")
				.addMapper(AirlineDepDelayDataMapper.class, "/airlines")
				.addCombiner(CarriersDataMapper.class)
				.addReducer(CarriersDataMapper.class)
				.setOutputfolder("/aircararrivaldelay")
				.setJobConf(jc)
				.build();
				
		List<DataCruncherContext> ctx = mdcjob.call();
	}
}
