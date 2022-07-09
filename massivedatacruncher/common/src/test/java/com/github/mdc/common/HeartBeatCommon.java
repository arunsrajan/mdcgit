package com.github.mdc.common;

import java.util.Properties;

import org.junit.BeforeClass;

public class HeartBeatCommon {

	@BeforeClass
	public static void initBefore() throws Exception {
		Properties prop = new Properties();
		prop.setProperty(MDCConstants.JGROUPSMCASTADDR, "238.10.10.10");
		prop.setProperty(MDCConstants.JGROUPSMCASTPORT, "46364");
		prop.setProperty(MDCConstants.CLUSTERNAME, "heartbeattestcluster1");
		prop.setProperty(MDCConstants.JGROUPSCONF, "../config/udp-largecluster.xml");
		MDCProperties.put(prop);
	}

	public HeartBeatCommon() {
	}


}
