package com.github.mdc.common;

import org.ehcache.CacheManager;

/**
 * 
 * @author arun
 * This class holds the CacheMaganer object for whole JVM.
 */
public class MDCCacheManager {
	private static CacheManager cachemanager = null;
	public static void put(CacheManager cachemanager) {
		MDCCacheManager.cachemanager = cachemanager;
	}
	public static CacheManager get() {
		return MDCCacheManager.cachemanager;
	}
}
