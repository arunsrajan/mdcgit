package com.github.mdc.common;

/**
 * 
 * @author Arun
 *
 * This class is like marker to close the Executor JGroups Channel
 */
public class CloseStagesGraphExecutor {
	
	public CloseStagesGraphExecutor(String jobid) {
		this.jobid = jobid;
	}
	
	private String jobid;

	public String getJobid() {
		return jobid;
	}

	public void setJobid(String jobid) {
		this.jobid = jobid;
	}
	
}
