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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

/**
 * Holds LaunchContainer object globally
 * @author arun
 *
 */
public class GlobalContainerLaunchers {

	private static Logger log = Logger.getLogger(GlobalContainerLaunchers.class);

	private GlobalContainerLaunchers() {
	}

	private static Map<String, List<LaunchContainers>> lcsmap = new ConcurrentHashMap<String, List<LaunchContainers>>();

	public static void put(String cid, List<LaunchContainers> lcs) {
		if (!lcsmap.containsKey(cid)) {
			lcsmap.put(cid, lcs);
		}
		else {
			log.info("Container Launched Already: " + cid + " With Resources: " + lcs);
		}
	}

	public static List<LaunchContainers> getAll() {
		return lcsmap.keySet().stream().flatMap(cid -> lcsmap.get(cid).stream()).collect(Collectors.toList());
	}

	public static List<LaunchContainers> get(String cid) {
		return lcsmap.get(cid);
	}

	public static void remove(String cid) {
		lcsmap.remove(cid);
	}
}
