package com.github.mdc.stream;

import java.io.Serializable;
import java.util.function.IntSupplier;

public class SampleSupplierPartition implements IntSupplier, Serializable {
	private static final long serialVersionUID = -2826097966250387651L;
	private Integer numsample;
	public SampleSupplierPartition(Integer numsample) {
		this.numsample = numsample;
	}
	
	@Override
	public int getAsInt() {
		return numsample;
	}
}
