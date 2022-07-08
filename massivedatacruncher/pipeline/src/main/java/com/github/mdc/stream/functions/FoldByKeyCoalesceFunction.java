package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.function.BinaryOperator;

@FunctionalInterface
public interface FoldByKeyCoalesceFunction<I> extends BinaryOperator<I>,Serializable {

}
