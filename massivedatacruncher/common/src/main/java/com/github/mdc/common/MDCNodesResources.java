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
