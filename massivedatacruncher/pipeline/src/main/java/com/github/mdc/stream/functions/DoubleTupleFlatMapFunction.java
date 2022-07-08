package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.List;

import org.jooq.lambda.tuple.Tuple2;

public interface DoubleTupleFlatMapFunction<I> extends Serializable {
	public abstract List<Tuple2<Double, Double>> apply(I i);
}
