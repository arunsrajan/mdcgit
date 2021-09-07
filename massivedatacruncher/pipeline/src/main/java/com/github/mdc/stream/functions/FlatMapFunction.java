package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.List;

public interface FlatMapFunction<I,O> extends Serializable{
	public abstract List<O> apply(I i);
}
