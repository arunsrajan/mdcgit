package com.github.mdc.stream.functions;

import java.io.Serializable;

@FunctionalInterface
public interface MapToPairFunction<I, O> extends MapFunction<I, O>, Serializable {
}
