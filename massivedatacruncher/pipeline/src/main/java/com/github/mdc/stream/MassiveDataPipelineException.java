package com.github.mdc.stream;

/**
 * 
 * @author arun
 * Exception class for the MassiveDataPipeline.
 */
public class MassiveDataPipelineException extends Exception {
	private static final long serialVersionUID = 2936875518462752063L;

	public MassiveDataPipelineException(String message) {
		super(message);
	}
	public MassiveDataPipelineException(String message,Exception ex) {
		super(message,ex);
	}
}
