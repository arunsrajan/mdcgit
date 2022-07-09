package com.github.mdc.stream;

import java.io.Serializable;
import java.util.function.IntSupplier;

/**
 * 
 * @author arun
 * The class holds the number of partitions.
 */
public class NumPartitions implements IntSupplier, Serializable {
	private Integer numpartition;
	private static final long serialVersionUID = 3903225461279785284L;

	public NumPartitions(Integer numpartition) {
		this.numpartition = numpartition;
	}

	@Override
	public int getAsInt() {
		return numpartition;
	}

}
