package com.github.mdc.stream;

import java.io.Serializable;
import java.util.function.IntSupplier;

public class NumPartitionsEachFile implements IntSupplier, Serializable{
	private Integer numpartition;
	private static final long serialVersionUID = 3903225461279785284L;
	
	public NumPartitionsEachFile(Integer numpartition) {
		this.numpartition = numpartition;
	}
	@Override
	public int getAsInt() {
		return numpartition;
	}
}
