/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
	public static enum TRIGGER {
		COUNT,COLLECT,SAVERESULTSTOFILE,FOREACH
	}
	public String id;
	public String containerid;
	public ConcurrentMap<Stage, Object> stageoutputmap;
	public ConcurrentMap<String, String> allstageshostport;
	public List<Stage> topostages = new Vector<>();
	public Long noofpartitions;
	public List<String> containers;
	public Set<String> nodes;
	public IgniteCache<Object, byte[]> igcache;
	public Ignite ignite;
	public List<Object> input = new Vector<>();
	public List<Object> output = new Vector<>();
	public Object results;
	public boolean isresultrequired;
	public JobMetrics jm;
	public PipelineConfig pipelineconfig;
	public boolean iscompleted;
	public Set vertices;
	public Set<DAGEdge> edges;
	public TRIGGER trigger;
	public String uri;
	public String savepath;
	public List<LaunchContainers> lcs;
}
