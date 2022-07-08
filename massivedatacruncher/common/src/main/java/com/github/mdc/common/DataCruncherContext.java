package com.github.mdc.common;

import java.io.Serializable;
import java.util.Collection;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

/**
 * 
 * @author arun
 * The implemention class for storing the key value pairs in MR api.
 * @param <K>
 * @param <V>
 */
@SuppressWarnings({"serial"})
public class DataCruncherContext<K, V> implements Context<K, V>, Serializable {

	private Map<K, Collection<V>> htkv = new Hashtable<>();

	@Override
	public void put(K k, V v) {
		if (htkv.get(k) == null) {
			htkv.put(k, new Vector<V>());
		}
		htkv.get(k).add(v);
	}

	@Override
	public Collection<V> get(K k) {
		return (Collection<V>) htkv.get(k);
	}

	@Override
	public Set<K> keys() {
		return htkv.keySet();
	}

	@Override
	public void addAll(K k, Collection<V> v) {
		if (htkv.get(k) != null) {
			htkv.get(k).addAll(v);
		} else if (v == null) {
			htkv.put(k, new Vector<V>());
		} else {
			htkv.put(k, v);
		}

	}

	@Override
	public void putAll(Set<K> k, V v) {
		k.stream().forEach(key -> {
			if (htkv.get(key) == null) {
				htkv.put(key, new LinkedHashSet<V>());
			}
			put(key, v);
		});
	}

	@Override
	public void add(Context<K, V> ctx) {
		ctx.keys().stream().forEach(key -> {
			addAll(key, ctx.get(key));
		});
		
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((htkv == null) ? 0 : htkv.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		DataCruncherContext other = (DataCruncherContext) obj;
		if (htkv == null) {
			if (other.htkv != null) {
				return false;
			}
		} else if (!htkv.equals(other.htkv)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "DataCruncherContext [htkv=" + htkv + "]";
	}

}
