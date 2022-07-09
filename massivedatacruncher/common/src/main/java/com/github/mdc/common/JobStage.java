package com.github.mdc.common;

import java.io.Serializable;

/**
 * 
 * @author Arun
 * The Holder of job with stage information and also the job statuses
 */
public class JobStage implements Serializable,Cloneable {
	private static final long serialVersionUID = 7292002084722232039L;
	public int currentstageindex;
	public String jobid;
	public String stageid;
	public Stage stage;
	public boolean isalreadyprocessed;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((jobid == null) ? 0 : jobid.hashCode());
		result = prime * result + ((stageid == null) ? 0 : stageid.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		JobStage other = (JobStage) obj;
		if (jobid == null) {
			if (other.jobid != null) {
				return false;
			}
		} else if (!jobid.equals(other.jobid)) {
			return false;
		}
		if (stageid == null) {
			if (other.stageid != null) {
				return false;
			}
		} else if (!stageid.equals(other.stageid)) {
			return false;
		}
		return true;
	}


	@Override
	public String toString() {
		return "JobStage [jobid=" + jobid + ", stageid=" + stageid + "]";
	}


	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}
}
