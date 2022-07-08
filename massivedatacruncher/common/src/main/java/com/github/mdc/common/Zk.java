package com.github.mdc.common;

public interface Zk<CF,I,K,V>{
	public abstract Object invoke(CF cf, I i, K k, V v);
}
