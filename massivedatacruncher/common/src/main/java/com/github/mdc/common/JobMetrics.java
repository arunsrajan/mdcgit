package com.github.mdc.common;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class JobMetrics {
	public String jobname;
	public String jobid;
	public List<String> files;
	public String mode;
	public double totalfilesize;
	public List<String> stages;
	public Map<String,Double> containersallocated;
	public Set<String> nodes;
	public long totalblocks;
	public long jobstarttime;
	public long jobcompletiontime;
	public double totaltimetaken;
	public List<String> stagecompletiontime;
}