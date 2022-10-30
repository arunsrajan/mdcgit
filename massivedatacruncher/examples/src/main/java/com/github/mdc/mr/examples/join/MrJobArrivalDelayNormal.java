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
package com.github.mdc.mr.examples.join;

import org.apache.log4j.Logger;

import com.github.mdc.common.JobConfiguration;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.tasks.scheduler.MapReduceApplication;
import com.github.mdc.tasks.scheduler.MapReduceApplicationBuilder;

public class MrJobArrivalDelayNormal implements com.github.mdc.tasks.scheduler.Application {
	static Logger log = Logger.getLogger(MrJobArrivalDelayNormal.class);

	@Override
	public void runMRJob(String[] args, JobConfiguration jobconfiguration) {
		jobconfiguration.setBatchsize(args[4]);
		jobconfiguration.setNumofreducers("1");
		jobconfiguration.setGctype(MDCConstants.ZGC);
		jobconfiguration.setExecmode(MDCConstants.EXECMODE_DEFAULT);
		jobconfiguration.setIsblocksuserdefined("true");
		jobconfiguration.setBlocksize(args[3]);
		var mdcjob = (MapReduceApplication) MapReduceApplicationBuilder.newBuilder()
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
