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
