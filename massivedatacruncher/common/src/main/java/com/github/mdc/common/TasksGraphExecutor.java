package com.github.mdc.common;

import java.util.List;

/**
 * 
 * @author arun
 * This class holds the information on the job stages with the 
 * hostport of the taskexecutors for jgroups
 * standalone execution mode.
 */
public class TasksGraphExecutor {
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
