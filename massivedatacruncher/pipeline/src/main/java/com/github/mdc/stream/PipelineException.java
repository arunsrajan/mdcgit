package com.github.mdc.stream;

/**
 * 
 * @author arun
 * Exception class for the MassiveDataPipeline.
 */
public class PipelineException extends Exception {
	private static final long serialVersionUID = 2936875518462752063L;

	public PipelineException(String message) {
		super(message);
	}
	public PipelineException(String message,Exception ex) {
		super(message,ex);
	}
}
