package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.BiPredicate;

@FunctionalInterface
public interface BiPredicateSerializable<I1, I2> extends BiPredicate<I1, I2>, Serializable {
	default BiPredicateSerializable<I1, I2> and(BiPredicateSerializable<I1, I2> other) {
		Objects.requireNonNull(other);
		return (t1, t2) -> test(t1, t2) && other.test(t1, t2);
	}

	default BiPredicateSerializable<I1, I2> or(BiPredicateSerializable<I1, I2> other) {
		Objects.requireNonNull(other);
		return (t1, t2) -> test(t1, t2) || other.test(t1, t2);
	}
}
