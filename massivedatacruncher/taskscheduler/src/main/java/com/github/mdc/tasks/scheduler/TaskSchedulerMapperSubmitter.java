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
import com.github.mdc.common.HeartBeatTaskScheduler;
import com.github.mdc.common.MDCConstants;
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
	HeartBeatTaskScheduler hbts;
	TaskSchedulerMapperSubmitter(Object blockslocation, boolean mapper, Set<String> mapperclasses,
			ApplicationTask apptask, CuratorFramework cf, List<String> containers,
			HeartBeatTaskScheduler hbts) {
		this.blockslocation = (BlocksLocation) blockslocation;
		this.mapper = mapper;
		this.mapperclasses = mapperclasses;
		this.apptask = apptask;
		this.cf = cf;
		this.containers = containers;
		this.hbts = hbts;
	}

	public BlocksLocation initializeobject(Set<String> mapperclasses, Set<String> combinerclasses)
			throws Exception, UnknownHostException, IOException {
		this.hostport = blockslocation.executorhp.split(MDCConstants.UNDERSCORE);
		blockslocation.mapperclasses = mapperclasses;
		blockslocation.combinerclasses = combinerclasses;
		return blockslocation;
	}

	public void sendChunk(BlocksLocation blockslocation) throws Exception {
		try {
			var objects = new ArrayList<>();
			objects.add(blockslocation);
			objects.add(apptask.applicationid);
			objects.add(apptask.taskid);
			Utils.writeObject(blockslocation.executorhp, objects);
		}
		catch(IOException ex) {
			var baos = new ByteArrayOutputStream();
			var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
			ex.printStackTrace(failuremessage);
			hbts.pingOnce(apptask, ApplicationTask.TaskStatus.FAILED, ApplicationTask.TaskType.MAPPERCOMBINER, new String(baos.toByteArray()));
			this.iscompleted = false;
		}
	}

	@Override
	public void setHostPort(String hp) {
		blockslocation.executorhp = hp;
	}

	@Override
	public String getHostPort() {
		return blockslocation.executorhp;
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