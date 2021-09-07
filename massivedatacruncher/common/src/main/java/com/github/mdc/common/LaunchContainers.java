package com.github.mdc.common;

import java.io.Serializable;

/**
 * 
 * @author arun
 * This class contains the fields for launching the task executors processed 
 * by the container. 
 */
public class LaunchContainers implements Serializable {

	private static final long serialVersionUID = 6845774609991683878L;
	public enum MODE{IGNITE,NORMAL};
	private String nodehostport;
	private String containerid;
	private String appid;
	private String jobid;
	private MODE mode;
	private ContainerLaunchAttributes cla;
	public String getContainerid() {
		return containerid;
	}
	public void setContainerid(String containerid) {
		this.containerid = containerid;
	}
	public ContainerLaunchAttributes getCla() {
		return cla;
	}
	public void setCla(ContainerLaunchAttributes cla) {
		this.cla = cla;
	}
	public String getAppid() {
		return appid;
	}
	public void setAppid(String appid) {
		this.appid = appid;
	}
	public String getJobid() {
		return jobid;
	}
	public void setJobid(String jobid) {
		this.jobid = jobid;
	}
	public MODE getMode() {
		return mode;
	}
	public void setMode(MODE mode) {
		this.mode = mode;
	}
	public String getNodehostport() {
		return nodehostport;
	}
	public void setNodehostport(String nodehostport) {
		this.nodehostport = nodehostport;
	}

	
}
