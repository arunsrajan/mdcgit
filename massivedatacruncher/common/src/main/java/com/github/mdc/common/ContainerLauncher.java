package com.github.mdc.common;

import java.util.ArrayList;

import org.apache.log4j.Logger;

/**
 * 
 * @author arun
 * The helper or utility class for launching container processes.
 */
public class ContainerLauncher {
	
	private ContainerLauncher() {}
	
	static Logger log = Logger.getLogger(ContainerLauncher.class);

	public static Process spawnMDCContainer(String port,
			String diskcache, Class<?> cls,String prop, ContainerResources cr) {

		try {
			var argumentsForSpawn = new ArrayList<String>();
			argumentsForSpawn.add(System.getProperty("java.home").replace("\\", "/") + "/bin/java");
			argumentsForSpawn.add("-classpath");
			argumentsForSpawn.add(System.getProperty("java.class.path"));			
			argumentsForSpawn.add("-Xms" + cr.getMinmemory() + "m");
			argumentsForSpawn.add("-Xmx" + cr.getMaxmemory() + "m");
			argumentsForSpawn.add("-XX:ActiveProcessorCount="+cr.getCpu());
			argumentsForSpawn.add("-XX:+HeapDumpOnOutOfMemoryError");
			argumentsForSpawn.add("--enable-preview");
			argumentsForSpawn.add("--add-opens=java.base/java.nio=ALL-UNNAMED");
			argumentsForSpawn.add("-Xrunjdwp:server=y,transport=dt_socket,address="+(Integer.parseInt(port)+100)+",suspend=n");
			argumentsForSpawn.add("-Djava.net.preferIPv4Stack=true");
			argumentsForSpawn.add(cr.getGctype());
			argumentsForSpawn.add("-D"+MDCConstants.TASKEXECUTOR_HOST+"=" + MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST));
			argumentsForSpawn.add("-D"+MDCConstants.TASKEXECUTOR_PORT+"=" + port);
			argumentsForSpawn.add("-D"+MDCConstants.CACHEDISKPATH+"=" + diskcache);
			argumentsForSpawn.add(cls.getName());
			argumentsForSpawn.add(prop);
			log.debug("Launching Container Daemon Process: "+argumentsForSpawn);
			var process = Runtime.getRuntime().exec(argumentsForSpawn.toArray(new String[argumentsForSpawn.size()]));
			return process;

		} catch (Exception ex) {
			log.error("Unable able to spawn container: " + cr.getMinmemory()+" "+cr.getMaxmemory()+" "+port+" "+cr.getCpu()+" "+cr.getGctype()+" "+cls.getName(),ex);
		}
		return null;
	}
	
	public static Process spawnMDCContainerIgnite(String port,
			String diskcache, Class<?> cls,String prop, ContainerResources cr) {
		try {
			var argumentsForSpawn = new ArrayList<String>();
			argumentsForSpawn.add(System.getProperty("java.home").replace("\\", "/") + "/bin/java");
			argumentsForSpawn.add("-classpath");
			argumentsForSpawn.add(System.getProperty("java.class.path"));			
			argumentsForSpawn.add("-Xms" + cr.getMinmemory() + "m");
			argumentsForSpawn.add("-Xmx" + cr.getMaxmemory() + "m");
			argumentsForSpawn.add("-XX:ActiveProcessorCount="+cr.getCpu());
			argumentsForSpawn.add("-XX:InitiatingHeapOccupancyPercent=80");
			argumentsForSpawn.add("-Xrunjdwp:server=y,transport=dt_socket,address="+(Integer.parseInt(port)+100)+",suspend=n");
			argumentsForSpawn.add(cr.getGctype());
			argumentsForSpawn.add(cls.getName());
			argumentsForSpawn.add(prop);
			argumentsForSpawn.add(port);
			log.debug("Launching Ignite Container Daemon Process: "+argumentsForSpawn);
			var process = Runtime.getRuntime().exec(argumentsForSpawn.toArray(new String[argumentsForSpawn.size()]));
			return process;

		} catch (Exception ex) {
			log.error("Unable able to spawn container: " + cr.getMinmemory()+" "+cr.getMaxmemory()+" "+port+" "+cr.getCpu()+" "+cr.getGctype()+" "+cls.getName(),ex);
		}
		return null;
	}
}
