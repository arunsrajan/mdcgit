package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.function.BiFunction;

@FunctionalInterface
public interface AggregateReduceFunction<T, U, R> extends BiFunction<T, U, R>, Serializable {

}
