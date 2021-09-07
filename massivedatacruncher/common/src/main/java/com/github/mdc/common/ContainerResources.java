package com.github.mdc.common;
/**
 * 
 * @author arun
 * The required container parameters or resources information for launching single container. 
 */
public class ContainerResources {
	private long minmemory;
	private long maxmemory;
	private String gctype;
	private long cpu;
	private int port;
	private boolean islaunched;
	public long getMinmemory() {
		return minmemory;
	}
	public void setMinmemory(long minmemory) {
		this.minmemory = minmemory;
	}
	public long getMaxmemory() {
		return maxmemory;
	}
	public void setMaxmemory(long maxmemory) {
		this.maxmemory = maxmemory;
	}
	public String getGctype() {
		return gctype;
	}
	public void setGctype(String gctype) {
		this.gctype = gctype;
	}
	public long getCpu() {
		return cpu;
	}
	public void setCpu(long cpu) {
		this.cpu = cpu;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public boolean isIslaunched() {
		return islaunched;
	}
	public void setIslaunched(boolean islaunched) {
		this.islaunched = islaunched;
	}
	
	
}
