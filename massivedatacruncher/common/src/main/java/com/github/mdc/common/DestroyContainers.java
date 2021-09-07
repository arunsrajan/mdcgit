package com.github.mdc.common;

import java.io.Serializable;

/**
 * 
 * @author arun
 * This class for destroying containers given containerid. 
 */
public class DestroyContainers implements Serializable {
	
	private static final long serialVersionUID = -1544266820256048905L;
	private String containerid;
	public String getContainerid() {
		return containerid;
	}
	public void setContainerid(String containerid) {
		this.containerid = containerid;
	}
	
}
