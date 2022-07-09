package com.github.mdc.common;

import org.apache.hadoop.hdfs.protocol.LocatedBlock;

/**
 * 
 * @author arun
 * This class holds the data for skipping to new line 
 * for the current located block with the datanode xref address. 
 */
public class SkipToNewLine {
	public LocatedBlock lblock;
	public long l;
	public String xrefaddress;

	@Override
	public String toString() {
		return "SkipToNewLine [lblock=" + lblock + ", l=" + l + ", xrefaddress=" + xrefaddress + "]";
	}
}
