package com.github.mdc.mr.examples.join;


import org.apache.log4j.Logger;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.tasks.scheduler.JobConfiguration;
import com.github.mdc.tasks.scheduler.MapReduceApplicationBuilder;
import com.github.mdc.tasks.scheduler.MapReduceApplicationYarn;

public class MrJobArrivalDelayYARN implements com.github.mdc.tasks.scheduler.Application {
	static String heapsize = "1024";
	static Logger log = Logger.getLogger(MrJobArrivalDelayYARN.class);
	@Override
	public void runMRJob(String[] args, JobConfiguration jobconfiguration) {
		jobconfiguration.setBatchsize("2");
		jobconfiguration.setNumofreducers("1");
		jobconfiguration.setGctype(MDCConstants.ZGC);
		jobconfiguration.setNumberofcontainers(args[3]);
		jobconfiguration.setMaxmem(args[4]);
		jobconfiguration.setIsblocksuserdefined("false");
		jobconfiguration.setExecmode(MDCConstants.EXECMODE_YARN);
		var mdcjob = (MapReduceApplicationYarn) MapReduceApplicationBuilder.newBuilder()
				.addMapper(CarriersDataMapper.class, args[1])
				.addMapper(AirlineArrDelayDataMapper.class, args[0])
				.addMapper(AirlineDepDelayDataMapper.class, args[0])			
				.addCombiner(CarriersDataMapper.class)
				.addReducer(CarriersDataMapper.class)
				.setOutputfolder(args[2])
				.setJobConf(jobconfiguration)
				.build();
		var ctx = mdcjob.call();
		log.info(ctx);
	}
}
