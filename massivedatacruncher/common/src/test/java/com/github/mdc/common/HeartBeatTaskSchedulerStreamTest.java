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

import java.util.UUID;

import org.junit.Test;

public class HeartBeatTaskSchedulerStreamTest extends HeartBeatCommon {

	public static final String stage = MDCConstants.STAGE;
	public static final String job = MDCConstants.JOB;
	public static final String task = MDCConstants.TASK;
	UUID jobid = UUID.randomUUID();

	@Test
	public void testHBTSSInitWithLessArgs() {
		try {
			HeartBeatTaskSchedulerStream hbtss = new HeartBeatTaskSchedulerStream();
			hbtss.init(2000, 1000, "127.0.0.1");
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_MESSAGE, ex.getMessage());
		}
	}


	@Test
	public void testHBTSSInitWithImproperRescheduleDelay() {
		try {
			HeartBeatTaskSchedulerStream hbtss = new HeartBeatTaskSchedulerStream();
			hbtss.init("224.0.0.1", 3000, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY, job + jobid.toString());
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_RESCHEDULE_DELAY, ex.getMessage());
		}
	}


	@Test
	public void testHBTSSInitWithImproperServerPort() {
		try {
			HeartBeatTaskSchedulerStream hbtss = new HeartBeatTaskSchedulerStream();
			hbtss.init(10000, "IMPROPERPORT", "127.0.0.1", 1000, 5000, MDCConstants.EMPTY, job + jobid.toString());
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_SERVER_PORT, ex.getMessage());
		}
	}

	@Test
	public void testHBTSSInitWithImproperHost() {
		try {
			HeartBeatTaskSchedulerStream hbtss = new HeartBeatTaskSchedulerStream();
			hbtss.init(10000, 2000, 1000, 1000, 5000, MDCConstants.EMPTY, job + jobid.toString());
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_SERVER_HOST, ex.getMessage());
		}
	}

	@Test
	public void testHBTSSInitWithImproperInitialDelay() {
		try {
			HeartBeatTaskSchedulerStream hbtss = new HeartBeatTaskSchedulerStream();
			hbtss.init(10000, 2000, "127.0.0.1", "IMPROPERINITIALDELAY", 5000, MDCConstants.EMPTY, job + jobid.toString());
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_INITIAL_DELAY, ex.getMessage());
		}
	}

	@Test
	public void testHBTSSInitWithImproperPingDelay() {
		try {
			HeartBeatServerStream hbss = new HeartBeatServerStream();
			hbss.init(10000, 2000, "127.0.0.1", 1000, "IMPROPERPINGDELAY", MDCConstants.EMPTY, job + jobid.toString());
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_PING_DELAY, ex.getMessage());
		}
	}

	@Test
	public void testHBTSSInitWithImproperContainerId() {
		try {
			HeartBeatServerStream hbss = new HeartBeatServerStream();
			hbss.init(10000, 2000, "127.0.0.1", 1000, 5000, 1000.0, job + jobid.toString());
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_CONTAINER_ID, ex.getMessage());
		}
	}


	@Test
	public void testHBTSSInitWithNoProperjobidType() {
		try {
			HeartBeatTaskSchedulerStream hbtss = new HeartBeatTaskSchedulerStream();
			hbtss.init(2000, 1000, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY, 100);
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_TASK_SCHEDULER_STREAM_EXCEPTON_JOBID, ex.getMessage());
		}
	}

	@Test
	public void testHBTSSStart() throws Exception {
		UUID stageid = UUID.randomUUID();
		HeartBeatTaskSchedulerStream hbtss = new HeartBeatTaskSchedulerStream();
		hbtss.init(2000, 1000, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY, job + jobid.toString());
		hbtss.start();
		hbtss.close();
	}

	@Test
	public void testHBTSSStartAndPing() throws Exception {
		UUID stageid = UUID.randomUUID();
		UUID taskid = UUID.randomUUID();
		HeartBeatTaskSchedulerStream hbtss = new HeartBeatTaskSchedulerStream();
		hbtss.init(10000, 2000, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY, job + jobid.toString());
		hbtss.start();

		HeartBeatTaskSchedulerStream hbtss1 = new HeartBeatTaskSchedulerStream();
		hbtss1.init(10000, 2001, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY, job + jobid.toString());
		hbtss1.pingOnce(stage + stageid.toString(), task + taskid.toString(), "127.0.0.1_2001", Task.TaskStatus.SUBMITTED, 1.0d, null);
		hbtss1.close();
		hbtss.close();
	}
}
