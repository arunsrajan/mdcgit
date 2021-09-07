package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.List;

public interface LongFlatMapFunction<I> extends Serializable {
	public abstract List<Long> apply(I i);
}
