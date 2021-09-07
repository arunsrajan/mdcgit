package com.github.mdc.common;

import java.io.Serializable;

/**
 * 
 * @author Arun
 * The resource information for scheduling.
 */
public class Resources implements Serializable{
	private static final long serialVersionUID = -1594977034575188825L;
	String nodeport;
	Long totalmemory;
	Long freememory;
	Integer numberofprocessors;
	Double totaldisksize;
	Double usabledisksize;
	Long physicalmemorysize;
	public String getNodeport() {
		return nodeport;
	}
	public void setNodeport(String nodeport) {
		this.nodeport = nodeport;
	}
	public Long getTotalmemory() {
		return totalmemory;
	}
	public void setTotalmemory(Long totalmemory) {
		this.totalmemory = totalmemory;
	}
	public Long getFreememory() {
		return freememory;
	}
	public void setFreememory(Long freememory) {
		this.freememory = freememory;
	}
	public Integer getNumberofprocessors() {
		return numberofprocessors;
	}
	public void setNumberofprocessors(Integer numberofprocessors) {
		this.numberofprocessors = numberofprocessors;
	}
	public Double getTotaldisksize() {
		return totaldisksize;
	}
	public void setTotaldisksize(Double totaldisksize) {
		this.totaldisksize = totaldisksize;
	}
	public Double getUsabledisksize() {
		return usabledisksize;
	}
	public void setUsabledisksize(Double usabledisksize) {
		this.usabledisksize = usabledisksize;
	}
	public Long getPhysicalmemorysize() {
		return physicalmemorysize;
	}
	public void setPhysicalmemorysize(Long physicalmemorysize) {
		this.physicalmemorysize = physicalmemorysize;
	}
	@Override
	public String toString() {
		return "Resources [nodeport=" + nodeport + ", totalmemory=" + totalmemory + ", freememory=" + freememory
				+ ", numberofprocessors=" + numberofprocessors + ", totaldisksize=" + totaldisksize
				+ ", usabledisksize=" + usabledisksize + ", physicalmemorysize=" + physicalmemorysize + "]";
	}
	
}
