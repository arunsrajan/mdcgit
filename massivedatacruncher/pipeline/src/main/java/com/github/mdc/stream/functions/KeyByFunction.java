package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.function.Function;

public interface KeyByFunction<T, R> extends Function<T, R>,Serializable {

}
