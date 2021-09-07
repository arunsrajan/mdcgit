package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Predicate;

@FunctionalInterface
public interface PredicateSerializable<O> extends Predicate<O>, Serializable {
	default PredicateSerializable<O> and(PredicateSerializable<? super O> other) {
        Objects.requireNonNull(other);
        return t -> test(t) && other.test(t);
    }
	default PredicateSerializable<O> or(PredicateSerializable<? super O> other) {
		Objects.requireNonNull(other);
        return t -> test(t) || other.test(t);
    }
}
