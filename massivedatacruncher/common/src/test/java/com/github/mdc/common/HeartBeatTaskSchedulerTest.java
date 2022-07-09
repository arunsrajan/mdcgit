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

import com.github.mdc.common.ApplicationTask.TaskStatus;
import com.github.mdc.common.ApplicationTask.TaskType;

public class HeartBeatTaskSchedulerTest extends HeartBeatCommon {
	UUID uuid = UUID.randomUUID();
	public static final String task = "Task";
	public static final String app = "App";

	@Test
	public void testHBTSInitWithLessArgs() {
		try {
			HeartBeatTaskScheduler hbts = new HeartBeatTaskScheduler();
			hbts.init(2000, 1000, "127.0.0.1");
		}
		catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_MESSAGE, ex.getMessage());
		}
	}

	@Test
	public void testHBTSInitWithImproperRescheduleDelay() {
		try {
			HeartBeatTaskScheduler hbts = new HeartBeatTaskScheduler();
			hbts.init("224.0.0.1", 2000, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY, app + "-" + uuid.toString(), task + "-" + uuid.toString());
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_RESCHEDULE_DELAY, ex.getMessage());
		}
	}

	@Test
	public void testHBTSInitWithImproperServerPort() {
		try {
			HeartBeatTaskScheduler hbts = new HeartBeatTaskScheduler();
			hbts.init(10000, "IMPROPERPORT", "127.0.0.1", 1000, 5000, MDCConstants.EMPTY, app + "-" + uuid.toString(), task + "-" + uuid.toString());
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_SERVER_PORT, ex.getMessage());
		}
	}

	@Test
	public void testHBTSInitWithImproperHost() {
		try {
			HeartBeatTaskScheduler hbts = new HeartBeatTaskScheduler();
			hbts.init(10000, 2000, 1000, 1000, 5000, MDCConstants.EMPTY, app + "-" + uuid.toString(), task + "-" + uuid.toString());
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_SERVER_HOST, ex.getMessage());
		}
	}

	@Test
	public void testHBTSInitWithImproperInitialDelay() {
		try {
			HeartBeatTaskScheduler hbts = new HeartBeatTaskScheduler();
			hbts.init(10000, 2000, "127.0.0.1", "IMPROPERINITIALDELAY", 5000, MDCConstants.EMPTY, app + "-" + uuid.toString(), task + "-" + uuid.toString());
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_INITIAL_DELAY, ex.getMessage());
		}
	}

	@Test
	public void testHBTSInitWithImproperPingDelay() {
		try {
			HeartBeatTaskScheduler hbts = new HeartBeatTaskScheduler();
			hbts.init(10000, 2000, "127.0.0.1", 1000, "IMPROPERPINGDELAY", MDCConstants.EMPTY, app + "-" + uuid.toString(), task + "-" + uuid.toString());
		} catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_EXCEPTION_PING_DELAY, ex.getMessage());
		}
	}


	@Test
	public void testHBTSInitWithNoProperAppIdType() {
		try {

			HeartBeatTaskScheduler hbts = new HeartBeatTaskScheduler();
			hbts.init(2000, 1000, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY, 2000, task + "-" + uuid.toString());
		}
		catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_TASK_SCHEDULER_EXCEPTION_APPID, ex.getMessage());
		}
	}

	@Test
	public void testHBTSInitWithNoProperTaskIdType() {
		try {
			HeartBeatTaskScheduler hbts = new HeartBeatTaskScheduler();
			hbts.init(2000, 1000, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY, app + "-" + uuid.toString(), 2000);
		}
		catch (Exception ex) {
			assertEquals(MDCConstants.HEARTBEAT_TASK_SCHEDULER_EXCEPTION_TASKID, ex.getMessage());
		}
	}

	@Test
	public void testHBTSServerStart() throws Exception {
		UUID appid = UUID.randomUUID();
		UUID taskid = UUID.randomUUID();
		HeartBeatTaskScheduler hbts = new HeartBeatTaskScheduler();
		hbts.init(2000, 1000, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY, app + "-" + appid.toString(), task + "-" + taskid.toString());
		hbts.start();
		hbts.stop();
		hbts.destroy();
		assertEquals(0, hbts.timermap.keySet().size());
	}

	@Test
	public void testHBTSStartAndPing() throws Exception {
		UUID appid = UUID.randomUUID();
		UUID taskid = UUID.randomUUID();
		System.setProperty("taskscheduler.initialdelay", "1000");
		System.setProperty("taskscheduler.rescheduledelay", "10000");
		HeartBeatTaskScheduler hbts = new HeartBeatTaskScheduler();
		hbts.init(1000, 1000, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY, app + "-" + appid.toString(), task + "-" + taskid.toString());
		hbts.start();

		HeartBeatTaskScheduler hbts1 = new HeartBeatTaskScheduler();
		hbts1.init(1000, 1001, "127.0.0.1", 1000, 5000, MDCConstants.EMPTY, app + "-" + appid.toString(), task + "-" + taskid.toString());
		hbts1.pingOnce(task + "-" + taskid.toString(), TaskStatus.COMPLETED, TaskType.MAPPERCOMBINER, null);

		hbts1.stop();
		hbts1.destroy();
		assertEquals(0, hbts1.timermap.keySet().size());
		hbts.stop();
		hbts.destroy();
		assertEquals(0, hbts.timermap.keySet().size());
	}

}
