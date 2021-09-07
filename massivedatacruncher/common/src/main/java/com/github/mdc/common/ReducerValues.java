package com.github.mdc.common;

import java.io.Serializable;
import java.util.List;

/**
 * 
 * @author Arun
 * Holder of reducer information for running 
 * the Reducer tasks in the task executor daemon.
 */
@SuppressWarnings("serial")
public class ReducerValues implements Serializable{
	public List tuples;
	public String reducerclass;
	public String appid;
	@Override
	public String toString() {
		return "ReducerValues [tuples=" + tuples + ", reducerclass=" + reducerclass + ", appid=" + appid + "]";
	}
}
