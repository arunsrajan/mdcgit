package com.github.mdc.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author arun
 *
 * The class with fields which contains various information for launching containers.
 */
public class ContainerLaunchAttributes implements Serializable {

	private static final long serialVersionUID = -949533421576520656L;
	private int numberofcontainers;
	List<ContainerResources> cr = new ArrayList<>();

	public int getNumberofcontainers() {
		return numberofcontainers;
	}

	public void setNumberofcontainers(int numberofcontainers) {
		this.numberofcontainers = numberofcontainers;
	}

	public List<ContainerResources> getCr() {
		return cr;
	}

	public void setCr(List<ContainerResources> cr) {
		this.cr = cr;
	}


}
