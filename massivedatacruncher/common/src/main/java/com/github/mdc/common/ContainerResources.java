/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.mdc.common;

/**
 * 
 * @author arun
 * The required container parameters or resources information for launching single container. 
 */
public class ContainerResources {
	private long minmemory;
	private long maxmemory;
	private long directheap;
	private String gctype;
	private int cpu;
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

	public int getCpu() {
		return cpu;
	}

	public void setCpu(int cpu) {
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

	public long getDirectheap() {
		return directheap;
	}

	public void setDirectheap(long directheap) {
		this.directheap = directheap;
	}


}
