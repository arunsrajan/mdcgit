package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.Comparator;

@FunctionalInterface
public interface SortedComparator<T1> extends Serializable, Comparator<T1> {

}
