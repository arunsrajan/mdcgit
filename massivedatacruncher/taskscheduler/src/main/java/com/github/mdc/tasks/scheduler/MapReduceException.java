package com.github.mdc.tasks.scheduler;

public class MapReduceException extends Exception {

	private static final long serialVersionUID = -7937152190279453002L;

	public static final String MEMORYALLOCATIONERROR = "Memory allocation error Minimum Required Memory is 128 MB";
	public static final String INSUFFMEMORYALLOCATIONERROR = "Insufficient memory, required memory is greater than the available memory";
	public static final String INSUFFNODESERROR = "Insufficient computing nodes";

	public MapReduceException(String message) {
		super(message);
	}
}
