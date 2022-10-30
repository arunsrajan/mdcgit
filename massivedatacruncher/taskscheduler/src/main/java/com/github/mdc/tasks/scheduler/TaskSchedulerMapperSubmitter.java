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
package com.github.mdc.tasks.scheduler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;

import com.github.mdc.common.ApplicationTask;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.RetrieveKeys;
import com.github.mdc.common.TaskSchedulerMapperSubmitterMBean;
import com.github.mdc.common.Utils;

public class TaskSchedulerMapperSubmitter implements TaskSchedulerMapperSubmitterMBean {
	BlocksLocation blockslocation;
	boolean mapper;
	Set<String> mapperclasses;
	ApplicationTask apptask;
	CuratorFramework cf;
	String hostport[];
	List<String> containers;
	Boolean iscompleted;

	TaskSchedulerMapperSubmitter(Object blockslocation, boolean mapper, Set<String> mapperclasses,
			ApplicationTask apptask, CuratorFramework cf, List<String> containers) {
		this.blockslocation = (BlocksLocation) blockslocation;
		this.mapper = mapper;
		this.mapperclasses = mapperclasses;
		this.apptask = apptask;
		this.cf = cf;
		this.containers = containers;
	}

	public BlocksLocation initializeobject(Set<String> mapperclasses, Set<String> combinerclasses)
			throws Exception, UnknownHostException, IOException {
		this.hostport = blockslocation.getExecutorhp().split(MDCConstants.UNDERSCORE);
		blockslocation.setMapperclasses(mapperclasses);
		blockslocation.setCombinerclasses(combinerclasses);
		return blockslocation;
	}

	public RetrieveKeys sendChunk(BlocksLocation blockslocation) throws Exception {
		try {
			var objects = new ArrayList<>();
			objects.add(blockslocation);
			objects.add(apptask.getApplicationid());
			objects.add(apptask.getTaskid());
			return (RetrieveKeys) Utils.getResultObjectByInput(blockslocation.getExecutorhp(), objects);
		}
		catch (IOException ex) {
			var baos = new ByteArrayOutputStream();
			var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
			ex.printStackTrace(failuremessage);
			this.iscompleted = false;
		}
		return null;
	}

	@Override
	public void setHostPort(String hp) {
		blockslocation.setExecutorhp(hp);
	}

	@Override
	public String getHostPort() {
		return blockslocation.getExecutorhp();
	}

	@Override
	public CuratorFramework getCuratorFramework() {
		return cf;
	}

	@Override
	public List<String> getContainers() {
		return containers;
	}
}
