package com.github.mdc.tasks.scheduler.yarn;

import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

/**
 * 
 * @author Arun
 * Job request object Hadoop yarn App Master and Yarn Container for MR pipelining API.
 */
public class JobRequest extends BaseObject {

	private State state;
	private byte[] job;
	private String containerid;
	private long timerequested;

	public JobRequest() {
	}

	public JobRequest(State state, byte[] job) {
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

	public long getTimerequested() {
		return timerequested;
	}

	public void setTimerequested(long timerequested) {
		this.timerequested = timerequested;
	}

	public enum State {
		WHATTODO,
		JOBDONE,
		JOBFAILED,
		RESPONSERECIEVED
	}

}
