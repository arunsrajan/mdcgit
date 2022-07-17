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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.jgroups.util.UUID;
import org.junit.Test;

public class HeartBeatServerStreamTest extends HeartBeatCommon {

	@Test
	public void testHeartBeatServerStreamInitWithNoArgs() {
		try {
			HeartBeatServerStream hbss = new HeartBeatServerStream();
			hbss.init();
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_MESSAGE, ex.getMessage());
		}
	}

	@Test
	public void testHeartBeatServerStreamInitWithImproperArgs() {
		try {
			HeartBeatServerStream hbss = new HeartBeatServerStream();
			hbss.init("224.0.0.1");
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_MESSAGE, ex.getMessage());
		}
	}

	@Test
	public void testHeartBeatServerStreamInitWithImproperRescheduleDelay() {
		try {
			HeartBeatServerStream hbss = new HeartBeatServerStream();
			hbss.init("224.0.0.1", 3000, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY);
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_RESCHEDULE_DELAY, ex.getMessage());
		}
	}


	@Test
	public void testHeartBeatServerStreamInitWithImproperServerPort() {
		try {
			HeartBeatServerStream hbss = new HeartBeatServerStream();
			hbss.init(10000, "IMPROPERPORT", "127.0.0.1", 1000, 5000, MDCConstants.EMPTY);
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_SERVER_PORT, ex.getMessage());
		}
	}

	@Test
	public void testHeartBeatServerStreamInitWithImproperHost() {
		try {
			HeartBeatServerStream hbss = new HeartBeatServerStream();
			hbss.init(10000, 2000, 1000, 1000, 5000, MDCConstants.EMPTY);
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_SERVER_HOST, ex.getMessage());
		}
	}

	@Test
	public void testHeartBeatServerStreamInitWithImproperInitialDelay() {
		try {
			HeartBeatServerStream hbss = new HeartBeatServerStream();
			hbss.init(10000, 2000, "127.0.0.1", "IMPROPERINITIALDELAY", 5000, MDCConstants.EMPTY);
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_INITIAL_DELAY, ex.getMessage());
		}
	}

	@Test
	public void testHeartBeatServerStreamInitWithImproperPingDelay() {
		try {
			HeartBeatServerStream hbss = new HeartBeatServerStream();
			hbss.init(10000, 2000, "127.0.0.1", 1000, "IMPROPERPINGDELAY", MDCConstants.EMPTY);
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_PING_DELAY, ex.getMessage());
		}
	}

	@Test
	public void testHeartBeatServerStreamInitWithImproperContainerId() {
		try {
			HeartBeatServerStream hbss = new HeartBeatServerStream();
			hbss.init(10000, 2000, "127.0.0.1", 1000, 5000, 1000.0);
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_CONTAINER_ID, ex.getMessage());
		}
	}

	@Test
	public void testHeartBeatServerStreamInitWithArgs() throws Exception {
		HeartBeatServerStream hbss = new HeartBeatServerStream();
		hbss.init(10000, 2000, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY);
		assertEquals(5000, hbss.pingdelay);
		assertEquals(2000, hbss.serverport);
		assertEquals("127.0.0.1", hbss.networkaddress);
	}

	@Test
	public void testHeartBeatServerStreamStart() throws Exception {
		HeartBeatServerStream hbss = new HeartBeatServerStream();
		hbss.init(10000, 2000, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY);
		System.setProperty(MDCConstants.CLUSTERNAME, "heartbeattestcluster");
		hbss.start();
		hbss.stop();
		hbss.destroy();
	}

	@Test
	public void testHeartBeatServerStreamStartAndPing() throws Exception {
		HeartBeatServerStream hbss = new HeartBeatServerStream();
		hbss.init(10000, 2000, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY);
		hbss.start();

		HeartBeatServerStream hbs1 = new HeartBeatServerStream();
		hbs1.init(10000, 2000, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY);
		hbs1.ping();

		while (hbss.hpresmap.keySet().size() != 1) {
			Thread.sleep(500);
		}
		;
		assertNotNull(hbss.hpresmap.get("127.0.0.1" + "_" + 2000));
		assertTrue(hbss.hpresmap.get("127.0.0.1" + "_" + 2000) instanceof Resources);
		Resources resources = (Resources) hbss.hpresmap.get("127.0.0.1" + "_" + 2000);
		assertEquals("127.0.0.1" + "_" + 2000, resources.getNodeport());
		hbs1.stop();
		hbs1.destroy();
		hbss.stop();
		hbss.destroy();
	}

	@Test
	public void testHeartBeatServerStreamStartAndPingContainerId() throws Exception {
		String containerid = UUID.randomUUID().toString();
		HeartBeatServerStream hbss = new HeartBeatServerStream();
		hbss.init(10000, 2000, "127.0.0.1", 1000, 5000, containerid);
		hbss.start();

		HeartBeatServerStream hbs1 = new HeartBeatServerStream();
		hbs1.init(10000, 2001, "127.0.0.1", 1000, 5000, containerid);
		hbs1.ping();

		while (Objects.isNull(hbss.containers) || !Objects.isNull(hbss.containers) && hbss.containers.size() < 1) {
			Thread.sleep(500);
		}
		;
		assertNotNull(hbss.containers);
		assertTrue(hbss.containers.contains("127.0.0.1_2001"));
		hbs1.stop();
		hbs1.destroy();
		hbss.stop();
		hbss.destroy();
	}

	@Test
	public void testHeartBeatMultipleServerStartAndPing() throws Exception {
		HeartBeatServerStream hbss = new HeartBeatServerStream();
		hbss.init(10000, 2000, "127.0.0.1", 1000, 1000, MDCConstants.EMPTY);
		hbss.start();
		int numberOfServers = 5;
		List<HeartBeatServerStream> heartBeatServerStreams = new ArrayList<>();
		int count = 1;
		while (count <= numberOfServers) {
			HeartBeatServerStream hbs1 = new HeartBeatServerStream();
			hbs1.init(10000, 2000 + count, "127.0.0.1", 1000, 1000, MDCConstants.EMPTY);
			hbs1.ping();
			heartBeatServerStreams.add(hbs1);
			count++;
		}

		while (hbss.hpresmap.keySet().size() != numberOfServers) {
			Thread.sleep(500);
		}
		;
		count = 1;
		while (count <= numberOfServers) {
			int port = 2000 + count;
			assertNotNull(hbss.hpresmap.get("127.0.0.1" + "_" + port));
			assertTrue(hbss.hpresmap.get("127.0.0.1" + "_" + port) instanceof Resources);
			Resources resources = (Resources) hbss.hpresmap.get("127.0.0.1" + "_" + port);
			assertEquals("127.0.0.1" + "_" + port, resources.getNodeport());
			count++;
		}
		count = 1;
		while (count <= numberOfServers) {
			HeartBeatServerStream hbs1 = heartBeatServerStreams.get(count - 1);
			hbs1.stop();
			hbs1.destroy();
			count++;
		}
		hbss.stop();
		hbss.destroy();
	}
}
