package com.github.mdc.common;

/**
 * 
 * @author arun
 * This class holds information number of times the datanode is allocated for a job.
 */
public class DatanodeXfefNumAllocated {
	String ref;
	int numallocated;
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + numallocated;
		result = prime * result + ((ref == null) ? 0 : ref.hashCode());
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
		DatanodeXfefNumAllocated other = (DatanodeXfefNumAllocated) obj;
		if (numallocated != other.numallocated)
			return false;
		if (ref == null) {
			if (other.ref != null)
				return false;
		} else if (!ref.equals(other.ref))
			return false;
		return true;
	}
	
	
}
