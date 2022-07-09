package com.github.mdc.common;

import java.util.List;

/**
 * 
 * @author Arun
 *
 * This class is like marker to close the Executor JGroups Channel
 */
public class CloseStagesGraphExecutor {

	public CloseStagesGraphExecutor(List<Task> tasks) {
		this.tasks = tasks;
	}

	private List<Task> tasks;

	public List<Task> getTasks() {
		return tasks;
	}

	public void setTasks(List<Task> tasks) {
		this.tasks = tasks;
	}

}
