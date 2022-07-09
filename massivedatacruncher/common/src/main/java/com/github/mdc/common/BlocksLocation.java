package com.github.mdc.common;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;

/**
 * 
 * @author Arun
 * This class is blocks information with the mapper and combiner class information
 */
public class BlocksLocation implements Serializable {
	private static final long serialVersionUID = 6205828696011624985L;
	public Block[] block = new Block[2];
	public String executorhp;
	public String xrefaddress;
	public Set<String> mapperclasses;
	public Set<String> combinerclasses;
	public Set<String> containers;

	@Override
	public String toString() {
		return "BlocksLocation [block=" + Arrays.toString(block) + "]";
	}

}
