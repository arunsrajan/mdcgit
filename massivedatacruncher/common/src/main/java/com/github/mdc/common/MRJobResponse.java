package com.github.mdc.common;

import java.io.Serializable;

/**
 * 
 * @author arun
 * This class receives the response from the task executors via heart beat 
 * for the MRJob API with the applicationid and taskid.
 */
public class MRJobResponse implements Serializable{
	private static final long serialVersionUID = -4255915804700326806L;
	private String appid;
	private String taskid;
	public String getAppid() {
		return appid;
	}
	public void setAppid(String appid) {
		this.appid = appid;
	}
	public String getTaskid() {
		return taskid;
	}
	public void setTaskid(String taskid) {
		this.taskid = taskid;
	}
	
}
