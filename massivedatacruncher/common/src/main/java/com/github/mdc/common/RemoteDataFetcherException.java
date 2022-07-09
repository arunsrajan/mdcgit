package com.github.mdc.common;

/**
 * 
 * @author arun
 * This class is exception class thrown when the RemoteDataFecth is executed
 * and also holds the error messages. 
 */
public class RemoteDataFetcherException extends Exception {

	private static final long serialVersionUID = 1121047245478192027L;

	public RemoteDataFetcherException(String message) {
		super(message);
	}

	RemoteDataFetcherException(String message, Exception ex) {
		super(message, ex);
	}

	static final String INTERMEDIATEPHASEWRITEERROR = "Write Intermediate Phase Output to DFS";
	static final String INTERMEDIATEPHASEREADERROR = "Read Intermediate Phase Output from DFS";
	static final String INTERMEDIATEPHASEDELETEERROR = "Delete Intermediate Phase Output from DFS";

}
