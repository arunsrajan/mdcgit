package com.github.mdc.common;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

public class GlobalContainerLaunchAttributes {

	private Logger log = Logger.getLogger(GlobalContainerLaunchAttributes.class);
	
	private GlobalContainerLaunchAttributes() {}
	
	Map<String,List<LaunchContainers>> lcsmap = new ConcurrentHashMap<String,List<LaunchContainers>>();
	
	public void put(String cidjobid,List<LaunchContainers> lcs) {
		if(!lcsmap.containsKey(cidjobid)) {
			lcsmap.put(cidjobid,lcs);
		}
		else {
			log.info("Container Launched Already: "+cidjobid+" With Resources: "+lcs);
		}
	}
	public List<LaunchContainers> get(String cidjobid) {
		return lcsmap.get(cidjobid);
	}
}
