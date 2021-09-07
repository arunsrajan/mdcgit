package com.github.mdc.stream;

import com.github.mdc.common.PipelineConfig;

/**
 * 
 * @author arun
 * The interface for the pipeline.
 */
public interface Pipeline {
	public abstract void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception;
}
