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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class GlobalContainerAllocDealloc {
	private static Map<String, List<String>> containercontainerids = new ConcurrentHashMap<>();
	private static Semaphore globalcontainerallocdeallocsem = new Semaphore(1);
	private static Map<String, ContainerResources> hportcrs = new ConcurrentHashMap<>();
	private static Map<String, String> containernode = new ConcurrentHashMap<>();
	private static Map<String, Set<String>> nodecontainers = new ConcurrentHashMap<>();

	public static Map<String, List<String>> getContainercontainerids() {
		return containercontainerids;
	}

	public static Semaphore getGlobalcontainerallocdeallocsem() {
		return globalcontainerallocdeallocsem;
	}


	public static Map<String, ContainerResources> getHportcrs() {
		return hportcrs;
	}

	public static Map<String, String> getContainernode() {
		return containernode;
	}

	public static Map<String, Set<String>> getNodecontainers() {
		return nodecontainers;
	}

	private GlobalContainerAllocDealloc() {
	}


}
