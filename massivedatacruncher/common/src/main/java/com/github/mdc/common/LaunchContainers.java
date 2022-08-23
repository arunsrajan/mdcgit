/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

	public enum MODE {
		IGNITE,NORMAL
	}
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

	@Override
	public String toString() {
		return "LaunchContainers [nodehostport=" + nodehostport + ", containerid=" + containerid + ", appid=" + appid
				+ ", jobid=" + jobid + ", mode=" + mode + ", cla=" + cla + "]";
	}


}
