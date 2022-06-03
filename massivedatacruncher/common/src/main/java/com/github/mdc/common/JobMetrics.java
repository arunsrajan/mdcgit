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
	public List<String> containerresources;
	public Map<String,Double> containersallocated;
	public Set<String> nodes;
	public long totalblocks;
	public long jobstarttime;
	public long jobcompletiontime;
	public double totaltimetaken;
	public List<String> stagecompletiontime;
	@Override
	public String toString() {
		return "JobMetrics [jobname=" + jobname + ", jobid=" + jobid + ", files=" + files + ", mode=" + mode
				+ ", totalfilesize=" + totalfilesize + ", stages=" + stages + ", containerresources="
				+ containerresources + ", containersallocated=" + containersallocated + ", nodes=" + nodes
				+ ", totalblocks=" + totalblocks + ", jobstarttime=" + jobstarttime + ", jobcompletiontime="
				+ jobcompletiontime + ", totaltimetaken=" + totaltimetaken + ", stagecompletiontime="
				+ stagecompletiontime + "]";
	}
}
