package com.github.mdc.common;

import java.io.Serializable;

/**
 * 
 * @author Arun
 * Holds the jar bytes by passing it to the task executor daemon 
 * by the task schedulers before executing the tasks  
 *
 */
public class LoadJar implements Serializable{
	private static final long serialVersionUID = 3379246177932569561L;
	public byte[] mrjar;
}
