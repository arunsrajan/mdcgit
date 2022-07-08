package com.github.mdc.stream.functions;

import java.util.Objects;

@FunctionalInterface
public interface LeftOuterJoinPredicate<I1,I2> extends BiPredicateSerializable<I1,I2> {
	default LeftOuterJoinPredicate<I1, I2> and(LeftOuterJoinPredicate<I1, I2> other) {
        Objects.requireNonNull(other);
        return (I1 t1, I2 t2) -> test(t1, t2) && other.test(t1, t2);
    }
	default LeftOuterJoinPredicate<I1, I2> or(LeftOuterJoinPredicate<I1, I2> other) {
		Objects.requireNonNull(other);
        return (I1 t1, I2 t2) -> test(t1, t2) || other.test(t1, t2);
    }
}
