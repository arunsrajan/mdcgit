package com.github.mdc.common;

/**
 * 
 * @author arun
 * This class is the request class to fetch information on the status of the tasks
 * during the stage execution from task executors.
 */
public class WhoIsRequest {

	private String stagepartitionid;

	public String getStagepartitionid() {
		return stagepartitionid;
	}

	public void setStagepartitionid(String stagepartitionid) {
		this.stagepartitionid = stagepartitionid;
	}
	
	
	
}
