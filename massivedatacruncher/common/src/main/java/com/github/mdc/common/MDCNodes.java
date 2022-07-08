package com.github.mdc.common;

import java.util.List;

/**
 * 
 * @author arun
 * This class holds the list of nodes available in the cluster 
 * stored by the schedulers for the entire JVM.
 */
public class MDCNodes {
	private static List<String> nodes;
	static void put(List<String> nodes) {
		MDCNodes.nodes = nodes;
	}
	public static List<String> get() {
		return MDCNodes.nodes;
	}

	private MDCNodes() {
	}
	
}
