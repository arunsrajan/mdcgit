package com.github.mdc.common;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class GlobalContainerAllocDealloc {
	private static Map<String,List<String>> containercontainerids = new ConcurrentHashMap<>();
	private static Semaphore globalcontainerallocdeallocsem = new Semaphore(1);
	private static Map<String,ContainerResources> hportcrs = new ConcurrentHashMap<>();
	private static Map<String,String> containernode = new ConcurrentHashMap<>();
	private static Map<String,Set<String>> nodecontainers = new ConcurrentHashMap<>();
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
