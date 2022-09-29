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
import java.util.List;

/**
 * 
 * @author arun
 * This class holds the information on the job stages with the 
 * hostport of the taskexecutors for jgroups
 * standalone execution mode.
 */
public class TasksGraphExecutor implements Serializable{
	private static final long serialVersionUID = -4313323243733293259L;
	private List<Task> tasks;
	private String hostport;

	public List<Task> getTasks() {
		return tasks;
	}

	public void setStages(List<Task> tasks) {
		this.tasks = tasks;
	}

	public String getHostport() {
		return hostport;
	}

	public void setHostport(String hostport) {
		this.hostport = hostport;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hostport == null) ? 0 : hostport.hashCode());
		result = prime * result + ((tasks == null) ? 0 : tasks.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		TasksGraphExecutor other = (TasksGraphExecutor) obj;
		if (hostport == null) {
			if (other.hostport != null) {
				return false;
			}
		} else if (!hostport.equals(other.hostport)) {
			return false;
		}
		if (tasks == null) {
			if (other.tasks != null) {
				return false;
			}
		} else if (!tasks.equals(other.tasks)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "TasksGraphExecutor [tasks=" + tasks  + ", hostport=" + hostport + "]";
	}

}
