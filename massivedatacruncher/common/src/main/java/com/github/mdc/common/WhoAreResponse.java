package com.github.mdc.common;

import java.util.Map;

/**
 * 
 * @author arun
 * This class is the response class with the status of the tasks
 * returned by the task executors to the job scheduler. 
 */
public class WhoAreResponse {
	private Map<String, WhoIsResponse.STATUS> responsemap;

	public Map<String, WhoIsResponse.STATUS> getResponsemap() {
		return responsemap;
	}

	public void setResponsemap(Map<String, WhoIsResponse.STATUS> responsemap) {
		this.responsemap = responsemap;
	}

}
