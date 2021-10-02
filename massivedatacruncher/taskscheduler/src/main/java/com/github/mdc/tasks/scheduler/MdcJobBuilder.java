package com.github.mdc.tasks.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.MDCConstants;

public class MdcJobBuilder {
	String jobname;
	JobConfiguration jobconf;
	List<MapperInput> mappers = new ArrayList<>();
	List<Class<?>> combiners = new ArrayList<>();
	List<Class<?>> reducers = new ArrayList<>();
	String outputfolder;

	private MdcJobBuilder() {

	}

	public static MdcJobBuilder newBuilder() {
		return new MdcJobBuilder();
	}

	@SuppressWarnings({ "rawtypes" })
	public MdcJobBuilder addMapper(Class crunchmapper, String inputfolderpath) {
		mappers.add(new MapperInput(crunchmapper, inputfolderpath));
		return this;
	}

	@SuppressWarnings({ "rawtypes" })
	public MdcJobBuilder addCombiner(Class crunchcombiner) {
		combiners.add(crunchcombiner);
		return this;
	}

	@SuppressWarnings({ "rawtypes" })
	public MdcJobBuilder addReducer(Class crunchreducer) {
		reducers.add(crunchreducer);
		return this;
	}

	public MdcJobBuilder setJobConf(JobConfiguration jobconf) {
		this.jobconf = jobconf;
		return this;
	}

	public MdcJobBuilder setOutputfolder(String outputfolder) {
		this.outputfolder = outputfolder;
		return this;
	}

	@SuppressWarnings("rawtypes")
	public Callable<List<DataCruncherContext>> build() {
		if (jobconf == null) {
			jobconf = JobConfigurationBuilder.newBuilder().build();
		}
		if(jobconf.getExecmode().equals(MDCConstants.EXECMODE_IGNITE)) {
			return new MdcJobIgnite(jobname, jobconf, mappers, combiners, reducers, outputfolder);
		}
		else if(jobconf.getExecmode().equals(MDCConstants.EXECMODE_YARN)) {
			return new MdcJobYarn(jobname, jobconf, mappers, combiners, reducers, outputfolder);
		}
		return new MdcJob(jobname, jobconf, mappers, combiners, reducers, outputfolder);
		
	}
}
