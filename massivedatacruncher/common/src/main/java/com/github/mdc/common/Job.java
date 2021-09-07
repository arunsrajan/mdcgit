package com.github.mdc.common;

import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentMap;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

/**
 * 
 * @author Arun
 * Holder of job information required to execute the job
 */
public class Job {
	public static enum TRIGGER{COUNT,COLLECT,SAVERESULTSTOFILE,FOREACH};
	public String id;
	public String containerid;
	public ConcurrentMap<Stage,Object> stageoutputmap;
	public ConcurrentMap<String,String> allstageshostport; 
	public List<Stage> topostages = new Vector<>();
	public Long noofpartitions;
	public List<String> containers;
	public Set<String> nodes;
	public IgniteCache<Object,byte[]> igcache;
	public Ignite ignite;
	public List<Object> input = new Vector<>();
	public List<Object> output = new Vector<>();
	public Object results;
	public boolean isresultrequired = false;
	public JobMetrics jm;
	public PipelineConfig pipelineconfig;
	public boolean iscompleted = false;
	public Set vertices;
	public Set<DAGEdge> edges;
	public TRIGGER trigger;
	public String uri;
	public String savepath;
	public List<LaunchContainers> lcs;
}
