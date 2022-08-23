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
 * @author Arun
 * Holder of zookeeper property information such as shards, replicas, file information, 
 * hosts contains shards and replicas and is chunk sparsed etc. 
 */
public class ZkChunkProp {
	private Integer numberofshards;
	private Integer numberofreplicas;
	private String files;
	private String hostsinvolved;
	private Boolean isdatasparsed;

	public Integer getNumberofshards() {
		return numberofshards;
	}

	public void setNumberofshards(Integer numberofshards) {
		this.numberofshards = numberofshards;
	}

	public Integer getNumberofreplicas() {
		return numberofreplicas;
	}

	public void setNumberofreplicas(Integer numberofreplicas) {
		this.numberofreplicas = numberofreplicas;
	}

	public String getFiles() {
		return files;
	}

	public void setFiles(String files) {
		this.files = files;
	}

	public String getHostsinvolved() {
		return hostsinvolved;
	}

	public void setHostsinvolved(String hostsinvolved) {
		this.hostsinvolved = hostsinvolved;
	}

	public Boolean getIsdatasparsed() {
		return isdatasparsed;
	}

	public void setIsdatasparsed(Boolean isdatasparsed) {
		this.isdatasparsed = isdatasparsed;
	}


}
