package com.github.mdc.stream.functions;

import java.io.Serializable;
import java.util.function.BiConsumer;

public interface AtomicBiConsumer<I1, I2> extends BiConsumer<I1, I1>, Serializable {

}
