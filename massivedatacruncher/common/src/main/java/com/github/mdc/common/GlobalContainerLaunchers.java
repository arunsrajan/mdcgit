package com.github.mdc.common;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;

public class GlobalContainerLaunchers {

	private static Logger log = Logger.getLogger(GlobalContainerLaunchers.class);
	
	private GlobalContainerLaunchers() {}
	
	private static Map<String,List<LaunchContainers>> lcsmap = new ConcurrentHashMap<String, List<LaunchContainers>>();
	
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
