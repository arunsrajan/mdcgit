package com.github.mdc.common;

import java.io.Serializable;
import java.util.Set;

/**
 * 
 * @author arun
 * This class holds the data storing the keys in the HDFS for
 *  the MR Job APi.
 */
public class RetrieveKeys implements Serializable {

	private static final long serialVersionUID = -100582342914768069L;

	public String applicationid;
	public String taskid;
	public Set<Object> keys;
	public boolean response;
}
