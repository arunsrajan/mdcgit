package com.github.mdc.stream.functions;

public class FoldByKey {
	private @SuppressWarnings("rawtypes")
	ReduceByKeyFunction reduceFunction;
	Object value;
	boolean left;
	@SuppressWarnings("rawtypes")
	public FoldByKey(Object value,ReduceByKeyFunction reduceFunction, boolean left){
		this.reduceFunction = reduceFunction;
		this.value = value;
		this.left = left;
	}
	@SuppressWarnings("rawtypes")
	public ReduceByKeyFunction getReduceFunction() {
		return reduceFunction;
	}
	@SuppressWarnings("rawtypes")
	public void setReduceFunction(ReduceByKeyFunction reduceFunction) {
		this.reduceFunction = reduceFunction;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
	public boolean isLeft() {
		return left;
	}
	public void setLeft(boolean left) {
		this.left = left;
	}
	
}
