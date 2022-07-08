package com.github.mdc.common;

import java.util.Collection;
import java.util.Set;

/**
 * 
 * @author Arun
 * The context interface for Map Reduce Framework Api
 * @param <K> Key
 * @param <V> Value
 */
public interface Context<K,V> {

	public void put(K k, V v);
	
	public void putAll(Set<K> k, V v);
	
	public Collection<V> get(K k);
	
	public Set<K> keys();
	
	public void addAll(K k, Collection<V> v);
	
	public void add(Context<K, V> ctx);
}
