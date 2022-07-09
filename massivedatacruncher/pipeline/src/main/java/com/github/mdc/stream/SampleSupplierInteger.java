package com.github.mdc.stream;

import java.io.Serializable;
import java.util.function.IntSupplier;

public class SampleSupplierInteger implements IntSupplier, Serializable {

	private static final long serialVersionUID = -2826097966250387651L;
	Integer numsample;

	public SampleSupplierInteger(Integer numsample) {
		this.numsample = numsample;
	}

	@Override
	public int getAsInt() {
		return numsample;
	}

}
