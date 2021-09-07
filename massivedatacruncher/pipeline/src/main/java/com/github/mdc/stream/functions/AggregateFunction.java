package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.function.BiFunction;

@FunctionalInterface
public interface AggregateFunction<T, U, R> extends Serializable, BiFunction<T, U, R> {

}
