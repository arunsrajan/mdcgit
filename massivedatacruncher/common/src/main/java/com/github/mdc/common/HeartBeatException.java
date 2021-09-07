package com.github.mdc.common;

/**
 * 
 * @author arun
 * The exception class for HeartBeat.
 */
public class HeartBeatException extends Exception {	
	private static final long serialVersionUID = -5494220769358236622L;

	HeartBeatException(String message) {
		super(message);
	}
}
