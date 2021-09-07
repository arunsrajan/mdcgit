package com.github.mdc.common;

import java.io.Serializable;

public class JobApp implements Serializable{
	private static final long serialVersionUID = -667681829064359241L;
	public enum JOBAPP{STREAM,MR};
	JOBAPP jobtype;
	private String containerid;
	private String jobappid;
	public String getContainerid() {
		return containerid;
	}
	public void setContainerid(String containerid) {
		this.containerid = containerid;
	}
	public String getJobappid() {
		return jobappid;
	}
	public void setJobappid(String jobappid) {
		this.jobappid = jobappid;
	}
	public JOBAPP getJobtype() {
		return jobtype;
	}
	public void setJobtype(JOBAPP jobtype) {
		this.jobtype = jobtype;
	}
	
}
