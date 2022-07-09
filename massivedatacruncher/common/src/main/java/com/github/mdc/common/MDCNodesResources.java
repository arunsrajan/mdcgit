package com.github.mdc.common;

import java.util.concurrent.ConcurrentMap;

/**
 * 
 * @author arun
 * This class holds the information resources 
 * of containers for launching the task executors. The key
 * is containers host with port with underscore as the token separator.
 * The values contains the resource information.
 */
public class MDCNodesResources {
	private MDCNodesResources() {
	}
	private static ConcurrentMap<String, Resources> resources;

	public static void put(ConcurrentMap<String, Resources> resources) {
		MDCNodesResources.resources = resources;
	}

	public static ConcurrentMap<String, Resources> get() {
		return MDCNodesResources.resources;
	}
}
