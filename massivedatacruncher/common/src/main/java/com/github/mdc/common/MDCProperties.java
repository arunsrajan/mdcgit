package com.github.mdc.common;

import java.util.Properties;

/**
 * 
 * @author arun
 * This class holds the properties of the Scheduler,Containers and task executor
 * which is stored for the entire JVM.
 */
public class MDCProperties {
	private MDCProperties() {
	}
	private static Properties properties;

	public static void put(Properties nodes) {
		MDCProperties.properties = nodes;
	}

	public static Properties get() {
		return MDCProperties.properties;
	}
}
