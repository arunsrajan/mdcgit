package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.function.BinaryOperator;

@FunctionalInterface
public interface CoalesceFunction<I> extends BinaryOperator<I>,Serializable {

}