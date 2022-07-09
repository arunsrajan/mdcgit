package com.github.mdc.common;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * 
 * @author Arun
 * File Split Block Information
 */
public class Block implements Serializable {
	private static final long serialVersionUID = 1641172215309142006L;
	public long blockOffset;
	public long blockstart;
	public long blockend;
	public String filename;
	public String hp;
	public Map<String, Set<String>> dnxref;

	@Override
	public String toString() {
		return "Block [blockOffset=" + blockOffset + ", blockstart=" + blockstart + ", blockend=" + blockend
				+ ", filename=" + filename + ", hp=" + hp + ", dnxref=" + dnxref + "]";
	}
}
