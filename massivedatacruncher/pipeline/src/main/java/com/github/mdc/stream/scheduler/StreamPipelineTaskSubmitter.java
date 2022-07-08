package com.github.mdc.stream.scheduler;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.github.mdc.common.Context;
import com.github.mdc.common.StreamPipelineTaskSubmitterMBean;
import com.github.mdc.common.Task;
import com.github.mdc.common.Utils;

/**
 * 
 * @author Arun
 * The task scheduler thread for submitting tasks using jobstage object.
 */
public class StreamPipelineTaskSubmitter implements StreamPipelineTaskSubmitterMBean,Callable<Object> {

	static Logger log = Logger.getLogger(StreamPipelineTaskSubmitter.class);
	private int level;
	private Task task;
	private String hp;
	private boolean completedexecution;
	private boolean resultobtainedte;
	public StreamPipelineTaskSubmitter() {}

	
	
	public boolean isCompletedexecution() {
		return completedexecution;
	}



	public void setCompletedexecution(boolean completedexecution) {
		this.completedexecution = completedexecution;
	}



	public StreamPipelineTaskSubmitter(Task task, String hp) {
		this.task = task;
		this.hp = hp;
	}
	
	public Task getTask() {
		return task;
	}

	public void setTask(Task task) {
		this.task = task;
	}

	/**
	 * Submit the job stages to task exectuors.
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public Context call() throws Exception {
		try {
			Utils.writeObject(hp, task);

		} catch (Exception ex) {
			log.error("Unable to connect and submit tasks to executor: ", ex);
			throw ex;
		}
		return null;
	}


	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((task == null) ? 0 : task.hashCode());
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
		StreamPipelineTaskSubmitter other = (StreamPipelineTaskSubmitter) obj;
		if (hp == null) {
			if (other.hp != null) {
				return false;
			}
		} else if (!hp.equals(other.hp)) {
			return false;
		}
		if (task == null) {
			if (other.task != null) {
				return false;
			}
		} else if (!task.equals(other.task)) {
			return false;
		}
		return true;
	}



	@Override
	public String toString() {
		return "StreamPipelineTaskSubmitter [task=" + task + ", hp=" + hp + ", completedexecution=" + completedexecution + "]";
	}



	@Override
	public void setHostPort(String hp) {
		this.hp = hp;
		
	}
	
	@Override
	public String getHostPort() {
		return hp;
	}



	public int getLevel() {
		return level;
	}



	public void setLevel(int level) {
		this.level = level;
	}



	public boolean isResultobtainedte() {
		return resultobtainedte;
	}



	public void setResultobtainedte(boolean resultobtainedte) {
		this.resultobtainedte = resultobtainedte;
	}
	
	
	
	
	
}
