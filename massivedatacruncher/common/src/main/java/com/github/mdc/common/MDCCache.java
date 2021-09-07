package com.github.mdc.common;

import org.ehcache.Cache;

/**
 * 
 * @author arun
 * This class holds the cache object for the whole JVM.
 */
public class MDCCache {
	private static Cache<?, ?> cache = null;
	public static void put(Cache<?, ?> cache) {
		MDCCache.cache = cache;
	}
	public static Cache<?, ?> get() {
		return MDCCache.cache;
	}
}
