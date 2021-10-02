package com.github.mdc.mr.examples.join;

import org.apache.log4j.Logger;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.tasks.scheduler.JobConfiguration;
import com.github.mdc.tasks.scheduler.MdcJob;
import com.github.mdc.tasks.scheduler.MdcJobBuilder;

public class MrJobArrivalDelayNormal implements com.github.mdc.tasks.scheduler.MRJob{
	static Logger log = Logger.getLogger(MrJobArrivalDelayNormal.class);
	@Override
	public void runMRJob(String[] args, JobConfiguration jobconfiguration) {
		jobconfiguration.setBatchsize(args[4]);
		jobconfiguration.setNumofreducers("1");
		jobconfiguration.setGctype(MDCConstants.ZGC);
		jobconfiguration.setExecmode(MDCConstants.EXECMODE_DEFAULT);
		jobconfiguration.setIsblocksuserdefined("true");
		jobconfiguration.setBlocksize(args[3]);
		var mdcjob = (MdcJob) MdcJobBuilder.newBuilder()
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
