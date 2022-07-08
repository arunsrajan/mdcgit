package com.github.mdc.tasks.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.MDCConstants;

public class MapReduceApplicationBuilder {
	String jobname;
	JobConfiguration jobconf;
	List<MapperInput> mappers = new ArrayList<>();
	List<Class<?>> combiners = new ArrayList<>();
	List<Class<?>> reducers = new ArrayList<>();
	String outputfolder;

	private MapReduceApplicationBuilder() {

	}

	public static MapReduceApplicationBuilder newBuilder() {
		return new MapReduceApplicationBuilder();
	}

	@SuppressWarnings({"rawtypes"})
	public MapReduceApplicationBuilder addMapper(Class crunchmapper, String inputfolderpath) {
		mappers.add(new MapperInput(crunchmapper, inputfolderpath));
		return this;
	}

	@SuppressWarnings({"rawtypes"})
	public MapReduceApplicationBuilder addCombiner(Class crunchcombiner) {
		combiners.add(crunchcombiner);
		return this;
	}

	@SuppressWarnings({"rawtypes"})
	public MapReduceApplicationBuilder addReducer(Class crunchreducer) {
		reducers.add(crunchreducer);
		return this;
	}

	public MapReduceApplicationBuilder setJobConf(JobConfiguration jobconf) {
		this.jobconf = jobconf;
		return this;
	}

	public MapReduceApplicationBuilder setOutputfolder(String outputfolder) {
		this.outputfolder = outputfolder;
		return this;
	}

	@SuppressWarnings("rawtypes")
	public Callable<List<DataCruncherContext>> build() {
		if (jobconf == null) {
			jobconf = JobConfigurationBuilder.newBuilder().build();
		}
		if (jobconf.getExecmode().equals(MDCConstants.EXECMODE_IGNITE)) {
			return new MapReduceApplicationIgnite(jobname, jobconf, mappers, combiners, reducers, outputfolder);
		}
		else if (jobconf.getExecmode().equals(MDCConstants.EXECMODE_YARN)) {
			return new MapReduceApplicationYarn(jobname, jobconf, mappers, combiners, reducers, outputfolder);
		}
		return new MapReduceApplication(jobname, jobconf, mappers, combiners, reducers, outputfolder);
		
	}
}
