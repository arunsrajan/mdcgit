package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.List;

public interface TupleFlatMapFunction<I, T> extends Serializable {
	public abstract List<T> apply(I i);
}
