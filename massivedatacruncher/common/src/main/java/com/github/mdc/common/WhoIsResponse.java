package com.github.mdc.common;

/**
 * 
 * @author arun
 * This class is the response class to fetch information on the status of the tasks
 * during the stage execution from task executors. This class is returned 
 * between task executors.
 */
public class WhoIsResponse {
	public static enum STATUS{YETTOSTART,RUNNING,COMPLETED,FAILED};
	private String stagepartitionid;
	private STATUS status;
	public String getStagepartitionid() {
		return stagepartitionid;
	}
	public void setStagepartitionid(String stagepartitionid) {
		this.stagepartitionid = stagepartitionid;
	}
	public STATUS getStatus() {
		return status;
	}
	public void setStatus(STATUS status) {
		this.status = status;
	}
	
}
