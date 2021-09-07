package com.github.mdc.common;

import java.io.Serializable;

/**
 * 
 * @author arun
 * The class for destroying task executors for a specific containers with the given host port.
 */
public class DestroyContainer implements Serializable {
	
	private static final long serialVersionUID = -1544266820256048905L;

	private String containerid;
	private String containerhp;
	public String getContainerid() {
		return containerid;
	}
	public void setContainerid(String containerid) {
		this.containerid = containerid;
	}
	public String getContainerhp() {
		return containerhp;
	}
	public void setContainerhp(String containerhp) {
		this.containerhp = containerhp;
	}
	
}
