package com.github.mdc.common;

/**
 * 
 * @author arun
 * This class is returned as response to the submitted job stage that job been received.
 */
public class JobStageResponse {
	String jobid;
	String stageid;
	public String getJobid() {
		return jobid;
	}
	public void setJobid(String jobid) {
		this.jobid = jobid;
	}
	public String getStageid() {
		return stageid;
	}
	public void setStageid(String stageid) {
		this.stageid = stageid;
	}
	
}
