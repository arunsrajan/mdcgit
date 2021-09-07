package com.github.mdc.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 
 * @author Arun
 * Stage information of the streaming MR api.
 */
public class Stage implements Serializable,Cloneable{

	private static final long serialVersionUID = 2272815602378403537L;
	public String id;
	private String stageid;
	public Integer number;
	public List<Object> tasks = new ArrayList<>();
	public Set<Stage> parent = new LinkedHashSet<>(),child = new LinkedHashSet<>();
	public Boolean isstagecompleted=false;
	public Boolean tovisit = true;
	public String tasksdescription="";
	@Override
	public Stage clone() throws CloneNotSupportedException {
		return (Stage) super.clone();
	}
	@Override
	public String toString() {
		return stageid+tasksdescription;
	}
	public String getStageid() {
		return stageid;
	}
	public void setStageid(String stageid) {
		this.stageid = stageid;
	}
	
	
}
