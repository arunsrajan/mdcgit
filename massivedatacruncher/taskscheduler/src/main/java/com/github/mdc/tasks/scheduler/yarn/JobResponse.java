package com.github.mdc.tasks.scheduler.yarn;

import org.springframework.yarn.integration.ip.mind.binding.BaseResponseObject;
/**
 * 
 * @author Arun
 * Job response object Hadoop yarn App Master and Yarn Container for MR pipelining API.
 */
public class JobResponse extends BaseResponseObject {

	private State state;
	private byte[] job;
	private String containerid;
	
	public JobResponse() {
	}

	public JobResponse(State state, byte[] job) {
		super();
		this.state = state;
		this.job = job;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}

	public byte[] getJob() {
		return job;
	}

	public void setJob(byte[] job) {
		this.job = job;
	}
	
	public String getContainerid() {
		return containerid;
	}

	public void setContainerid(String containerid) {
		this.containerid = containerid;
	}



	public enum State {
		DIE,
		STANDBY,
		RUNJOB
	}

}
