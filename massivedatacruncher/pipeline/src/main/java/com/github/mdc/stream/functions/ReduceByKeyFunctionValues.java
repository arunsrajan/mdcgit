package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.function.BinaryOperator;

@FunctionalInterface
public interface ReduceByKeyFunctionValues<I1> extends BinaryOperator<I1>,Serializable {

}
