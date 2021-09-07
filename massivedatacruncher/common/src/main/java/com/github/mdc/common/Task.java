package com.github.mdc.common;

import java.io.Serializable;
import java.util.List;

import com.github.mdc.common.MDCConstants.STORAGE;

/**
 * 
 * @author Arun
 * Holds the task information such as execution function, parent and child tasks 
 * in the form graph for the streaming API.
 */
public class Task implements Serializable,Cloneable{
	private static final long serialVersionUID = 4608751332110497234L;
	
	public Task() {
		super();
	}
	public Object[] input = null;
	public RemoteDataFetch[] parentremotedatafetch = null;
	public enum TaskStatus{YETTOSUBMIT,SUBMITTED,RUNNING,COMPLETED,FAILED};
	public TaskStatus taskstatus;
	public boolean visited;
	public String jobid;
	public String stageid;
	public String taskid=MDCConstants.TASK+MDCConstants.HYPHEN+Utils.getUniqueID();
	public String hostport;
	public String stagefailuremessage;
	public double timetakenseconds;
	private String taskname;
	public STORAGE storage;
	public List<Task> taskspredecessor;
	
	
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((taskid == null) ? 0 : taskid.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Task other = (Task) obj;
		if (taskid == null) {
			if (other.taskid != null)
				return false;
		} else if (!taskid.equals(other.taskid))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "Task [taskid=" + taskid + "]";
	}

	public String getTaskname() {
		return taskname;
	}

	public void setTaskname(String taskname) {
		this.taskname = taskname;
	}

	@Override
	public Object clone() throws CloneNotSupportedException{
		return super.clone();
		
	}
	
}