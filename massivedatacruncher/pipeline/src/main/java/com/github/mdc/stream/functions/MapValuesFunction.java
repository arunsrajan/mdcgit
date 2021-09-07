package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.function.Function;

@FunctionalInterface
public interface MapValuesFunction<I1, O1> extends Function<I1, O1>,Serializable {

}
