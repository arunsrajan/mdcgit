package com.github.mdc.common;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 
 * @author arun
 * This class holds the information resources 
 * of containers for launching the task executors. The key
 * is containers host with port with underscore as the token separator.
 * The values contains the resource information.
 */
public class MDCNodesResourcesSnapshot {
	private MDCNodesResourcesSnapshot() {}
	private static ConcurrentMap<String,Resources> resources = new ConcurrentHashMap<>();
	public static void put(ConcurrentMap<String,Resources> resources) {
		MDCNodesResourcesSnapshot.resources.putAll(resources);
	}
	public static ConcurrentMap<String,Resources> get() {
		return MDCNodesResourcesSnapshot.resources;
	}
}
