package com.github.mdc.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MDCJobMetrics {
	private MDCJobMetrics() {
	}
	static Map<String, JobMetrics> jms = new ConcurrentHashMap<String, JobMetrics>();

	public static void put(JobMetrics jm) {
		jms.put(jm.jobid, jm);
	}

	public static Map<String, JobMetrics> get() {
		return jms;
	}

	public static JobMetrics get(String id) {
		return jms.get(id);
	}

}
