package com.github.mdc.common;

import java.io.Serializable;

/**
 * 
 * @author Arun
 * Retrieve the result of the streaming api job execution.
 */
public class FreeResourcesCompletedJob implements Serializable {
	public String jobid;
	public String containerid;
	private static final long serialVersionUID = -8474439567106417669L;
}
