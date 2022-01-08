package com.github.mdc.common;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 
 * @author Arun
 * The Holder of job and stage information to receive the final stage output.
 */
public class RemoteDataFetch implements Serializable{
	private static final long serialVersionUID = 2952764365767007054L;
	public String jobid;
	public String stageid;
	public String taskid;
	public String hp;
	public byte[] data;
	public String mode;
	@Override
	public String toString() {
		return "RemoteDataFetch [jobid=" + jobid + ", stageid=" + stageid + ", taskid=" + taskid + ", hp=" + hp
				+ ", data=" + Arrays.toString(data) + ", mode=" + mode + "]";
	}
}
