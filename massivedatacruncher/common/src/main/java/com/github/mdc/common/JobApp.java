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

public class JobApp implements Serializable {
	private static final long serialVersionUID = -667681829064359241L;

	public enum JOBAPP {
		STREAM,MR
	}
	JOBAPP jobtype;
	private String containerid;
	private String jobappid;

	public String getContainerid() {
		return containerid;
	}

	public void setContainerid(String containerid) {
		this.containerid = containerid;
	}

	public String getJobappid() {
		return jobappid;
	}

	public void setJobappid(String jobappid) {
		this.jobappid = jobappid;
	}

	public JOBAPP getJobtype() {
		return jobtype;
	}

	public void setJobtype(JOBAPP jobtype) {
		this.jobtype = jobtype;
	}

}
