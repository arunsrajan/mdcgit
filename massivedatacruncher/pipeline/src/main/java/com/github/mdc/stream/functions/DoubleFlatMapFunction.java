package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.List;

public interface DoubleFlatMapFunction<I> extends Serializable {
	public abstract List<Double> apply(I i);
}
