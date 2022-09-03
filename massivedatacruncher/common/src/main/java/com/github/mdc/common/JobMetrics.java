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

import lombok.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
@EqualsAndHashCode
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class JobMetrics {
	public String jobname;
	public String jobid;
	public List<String> files;
	public String mode;
	public double totalfilesize;
	public List<String> stages;
	public List<String> containerresources;
	public Map<String, Double> containersallocated;
	public Set<String> nodes;
	public long totalblocks;
	public long jobstarttime;
	public long jobcompletiontime;
	public double totaltimetaken;
	public List<String> stagecompletiontime;
	public Map<String, List<Task>> taskexcutortasks = new ConcurrentHashMap<>();

}
