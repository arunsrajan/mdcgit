package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface MapFunction<T, R> extends Function<T, R>,Serializable {

}
