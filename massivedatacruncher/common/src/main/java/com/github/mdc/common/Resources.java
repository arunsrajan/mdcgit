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

import java.io.Serializable;

/**
 * 
 * @author Arun
 * The resource information for scheduling.
 */
public class Resources implements Serializable {
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
