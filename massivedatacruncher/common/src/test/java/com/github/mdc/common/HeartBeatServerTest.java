package com.github.mdc.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.jgroups.util.UUID;
import org.junit.Test;

import com.github.mdc.common.HeartBeatServer;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.Resources;

public class HeartBeatServerTest extends HeartBeatCommon {

	@Test
	public void testHeartBeatServerInitWithNoArgs() {
		try {
			HeartBeatServer hbs = new HeartBeatServer();
			hbs.init();
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_MESSAGE, ex.getMessage());
		}
	}

	@Test
	public void testHeartBeatServerInitWithImproperArgs() {
		try {
			HeartBeatServer hbs = new HeartBeatServer();
			hbs.init("224.0.0.1");
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_MESSAGE, ex.getMessage());
		}
	}

	@Test
	public void testHeartBeatServerInitWithImproperRescheduleDelay() {
		try {
			HeartBeatServer hbs = new HeartBeatServer();
			hbs.init("224.0.0.1", 2000, "127.0.0.1", 1000, 5000,MDCConstants.EMPTY);
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_RESCHEDULE_DELAY, ex.getMessage());
		}
	}

	@Test
	public void testHeartBeatServerInitWithImproperServerPort() {
		try {
			HeartBeatServer hbs = new HeartBeatServer();
			hbs.init(10000, "IMPROPERPORT", "127.0.0.1", 1000, 5000,MDCConstants.EMPTY);
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_SERVER_PORT, ex.getMessage());
		}
	}

	@Test
	public void testHeartBeatServerInitWithImproperHost() {
		try {
			HeartBeatServer hbs = new HeartBeatServer();
			hbs.init(10000, 2000, 1000, 1000, 5000,MDCConstants.EMPTY);
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_SERVER_HOST, ex.getMessage());
		}
	}
	
	@Test
	public void testHeartBeatServerInitWithImproperInitialDelay() {
		try {
			HeartBeatServer hbs = new HeartBeatServer();
			hbs.init(10000, 2000, "127.0.0.1", "IMPROPERINITIALDELAY", 5000,MDCConstants.EMPTY);
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_INITIAL_DELAY, ex.getMessage());
		}
	}
	@Test
	public void testHeartBeatServerInitWithImproperPingDelay() {
		try {
			HeartBeatServer hbs = new HeartBeatServer();
			hbs.init(10000, 2000, "127.0.0.1", 1000, "IMPROPERPINGDELAY",MDCConstants.EMPTY);
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_PING_DELAY, ex.getMessage());
		}
	}
	@Test
	public void testHeartBeatServerInitWithImproperContainerId() {
		try {
			HeartBeatServer hbs = new HeartBeatServer();
			hbs.init(10000, 2000, "127.0.0.1", 1000, 5000,1000.0);
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_CONTAINER_ID, ex.getMessage());
		}
	}

	@Test
	public void testHeartBeatServerInitWithArgs() throws Exception {
		HeartBeatServer hbs = new HeartBeatServer();
		hbs.init(10000, 2000, "127.0.0.1", 1000, 5000,MDCConstants.EMPTY);
		assertNotNull(hbs.pingdelay);
		assertEquals(5000, hbs.pingdelay);
		assertNotNull(hbs.serverport);
		assertEquals(2000, hbs.serverport);
		assertNotNull(hbs.networkaddress);
		assertEquals("127.0.0.1", hbs.networkaddress);
		assertNotNull(hbs.threadpool);
	}

	@Test
	public void testHeartBeatServerStart() throws Exception {
		HeartBeatServer hbs = new HeartBeatServer();
		hbs.init(10000, 2000, "127.0.0.1", 1000, 5000,MDCConstants.EMPTY);
		System.setProperty(MDCConstants.CLUSTERNAME, "heartbeattestcluster");
		hbs.start();
		assertNotNull(hbs.scheduledthreadpool);
		hbs.stop();
		hbs.destroy();
		assertEquals(0, hbs.timermap.keySet().size());
		assertEquals(true, hbs.threadpool.isShutdown());
		assertEquals(true, hbs.threadpool.isTerminated());
		assertEquals(true, hbs.scheduledthreadpool.isShutdown());
		assertEquals(true, hbs.scheduledthreadpool.isTerminated());
	}

	@Test
	public void testHeartBeatServerStartAndPing() throws Exception {
		System.setProperty(MDCConstants.CLUSTERNAME, "heartbeattestcluster1");
		HeartBeatServer hbs = new HeartBeatServer();
		hbs.init(10000, 2000, "127.0.0.1", 1000, 5000,MDCConstants.EMPTY);
		hbs.start();

		HeartBeatServer hbs1 = new HeartBeatServer();
		hbs1.init(10000, 2001, "127.0.0.1", 1000, 5000,MDCConstants.EMPTY);
		hbs1.ping();

		while(hbs.hpresmap.keySet().size()!=1) {
			Thread.sleep(500);
		};
		assertNotNull(hbs.hpresmap.get("127.0.0.1" + "_" + 2001));
		assertTrue(hbs.hpresmap.get("127.0.0.1" + "_" + 2001) instanceof Resources);
		Resources resources = (Resources) hbs.hpresmap.get("127.0.0.1" + "_" + 2001);
		assertEquals("127.0.0.1" + "_" + 2001, resources.getNodeport());
		assertNotNull(hbs.scheduledthreadpool);
		hbs1.stop();
		hbs1.destroy();
		hbs.stop();
		hbs.destroy();
		assertEquals(0, hbs.timermap.keySet().size());
		assertEquals(true, hbs.threadpool.isShutdown());
		assertEquals(true, hbs.threadpool.isTerminated());
		assertEquals(true, hbs.scheduledthreadpool.isShutdown());
		assertEquals(true, hbs.scheduledthreadpool.isTerminated());
		assertEquals(0, hbs1.timermap.keySet().size());
		assertEquals(true, hbs1.threadpool.isShutdown());
		assertEquals(true, hbs1.threadpool.isTerminated());
	}
	
	@Test
	public void testHeartBeatServerStartAndPingContainerId() throws Exception {
		String containerid = UUID.randomUUID().toString();
		HeartBeatServer hbs = new HeartBeatServer();
		hbs.init(10000, 2000, "127.0.0.1", 1000, 5000,containerid);
		hbs.start();

		HeartBeatServer hbs1 = new HeartBeatServer();
		hbs1.init(10000, 2001, "127.0.0.1", 1000, 5000,containerid);
		hbs1.ping();

		while(Objects.isNull(hbs.containers)||!Objects.isNull(hbs.containers)&&hbs.containers.size()<1) {
			Thread.sleep(500);
		};
		assertNotNull(hbs.containers);
		assertTrue(hbs.containers.contains("127.0.0.1_2001"));
		assertNotNull(hbs.scheduledthreadpool);
		hbs1.stop();
		hbs1.destroy();
		hbs.stop();
		hbs.destroy();
		assertEquals(0, hbs.timermap.keySet().size());
		assertEquals(true, hbs.threadpool.isShutdown());
		assertEquals(true, hbs.threadpool.isTerminated());
		assertEquals(true, hbs.scheduledthreadpool.isShutdown());
		assertEquals(true, hbs.scheduledthreadpool.isTerminated());
		assertEquals(0, hbs1.timermap.keySet().size());
		assertEquals(true, hbs1.threadpool.isShutdown());
		assertEquals(true, hbs1.threadpool.isTerminated());
	}
	@Test
	public void testHeartBeatMultipleServerStartAndPing() throws Exception {
		HeartBeatServer hbss = new HeartBeatServer();
		hbss.init(10000, 2000, "127.0.0.1", 1000, 1000,MDCConstants.EMPTY);
		hbss.start();
		int numberOfServers = 3;
		List<HeartBeatServer> heartBeatServer = new ArrayList<>();
		int count = 1;
		while (count <= numberOfServers) {
			HeartBeatServer hbs1 = new HeartBeatServer();
			hbs1.init(10000, 2000 + count, "127.0.0.1", 1000, 1000,MDCConstants.EMPTY);
			hbs1.ping();
			heartBeatServer.add(hbs1);
			count++;
		}

		while(hbss.hpresmap.keySet().size()!=numberOfServers) {
			Thread.sleep(500);
		};
		count = 1;
		while (count <= numberOfServers) {
			HeartBeatServer hbs1 = heartBeatServer.get(count - 1);
			int port = 2000 + count;
			assertNotNull(hbss.hpresmap.get("127.0.0.1" + "_" + port));
			assertTrue(hbss.hpresmap.get("127.0.0.1" + "_" + port) instanceof Resources);
			Resources resources = (Resources) hbss.hpresmap.get("127.0.0.1" + "_" + port);
			assertEquals("127.0.0.1" + "_" + port, resources.getNodeport());
			assertNotNull(hbss.scheduledthreadpool);
			count++;
		}
		count = 1;
		while (count <= numberOfServers) {
			HeartBeatServer hbs1 = heartBeatServer.get(count - 1);
			hbs1.stop();
			hbs1.destroy();
			count++;
		}
		hbss.stop();
		hbss.destroy();
		assertEquals(0, hbss.timermap.keySet().size());
		assertEquals(true, hbss.threadpool.isShutdown());
		assertEquals(true, hbss.threadpool.isTerminated());
		assertEquals(true, hbss.scheduledthreadpool.isShutdown());
		assertEquals(true, hbss.scheduledthreadpool.isTerminated());

	}
}
