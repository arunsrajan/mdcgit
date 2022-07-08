package com.github.mdc.common;

import java.io.Serializable;

/**
 * 
 * @author Arun
 * The Heartbeat response between task scheduler and task executor of map reduce framework.
 */
public class ApplicationTask implements Serializable {

	private static final long serialVersionUID = -3860457009382190198L;
	//This enum declaration are the statuses of the Map Reduce Framework Executor and Task Scheduler Heart Beat
	public enum TaskStatus {YETTOSUBMIT,SUBMITTED,RUNNING,COMPLETED,FAILED};
	
	//This enum declaration are the task type for Map Reduce Framework.
	public enum TaskType {REDUCER,MAPPERCOMBINER};
	
	public String applicationid;
	public String taskid;
	public TaskStatus taskstatus;
	public TaskType tasktype;
	public String hp;
	public String apperrormessage;
	
	
	public String getHp() {
		return hp;
	}
	public TaskType getTasktype() {
		return tasktype;
	}
	
	
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((applicationid == null) ? 0 : applicationid.hashCode());
		result = prime * result + ((hp == null) ? 0 : hp.hashCode());
		result = prime * result + ((taskid == null) ? 0 : taskid.hashCode());
		result = prime * result + ((taskstatus == null) ? 0 : taskstatus.hashCode());
		result = prime * result + ((tasktype == null) ? 0 : tasktype.hashCode());
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
		ApplicationTask other = (ApplicationTask) obj;
		if (applicationid == null) {
			if (other.applicationid != null) {
				return false;
			}
		} else if (!applicationid.equals(other.applicationid)) {
			return false;
		}
		if (hp == null) {
			if (other.hp != null) {
				return false;
			}
		} else if (!hp.equals(other.hp)) {
			return false;
		}
		if (taskid == null) {
			if (other.taskid != null) {
				return false;
			}
		} else if (!taskid.equals(other.taskid)) {
			return false;
		}
		if (taskstatus != other.taskstatus) {
			return false;
		}
		if (tasktype != other.tasktype) {
			return false;
		}
		return true;
	}
	
	@Override
	public String toString() {
		return "ApplicationTask [applicationid=" + applicationid + ", taskid=" + taskid + ", taskstatus=" + taskstatus
				+ ", tasktype=" + tasktype + ", hp=" + hp + "]";
	}
	
}
