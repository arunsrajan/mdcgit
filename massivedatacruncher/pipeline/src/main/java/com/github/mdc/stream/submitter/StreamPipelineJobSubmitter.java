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
package com.github.mdc.stream.submitter;

import static java.util.Objects.nonNull;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URL;
import java.util.Objects;
import java.util.Random;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.log4j.Logger;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.Utils;
import com.github.mdc.common.ZookeeperOperations;

/**
 * 
 * @author Arun
 * Submit the Map Reduce stream pipelining API jobs.
 */
public class StreamPipelineJobSubmitter {

	static Logger log = Logger.getLogger(StreamPipelineJobSubmitter.class);

	/**
	 * Main method for sumbitting the MR jobs.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, MDCConstants.MDC_PROPERTIES);
		try (var cf = CuratorFrameworkFactory.newClient(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_HOSTPORT),
				20000, 50000, new RetryForever(2000));) {
			cf.start();
			var hostport = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOSTPORT);
			var taskscheduler = (String) ZookeeperOperations.nodedata.invoke(cf,
					MDCConstants.FORWARD_SLASH + MDCProperties.get().getProperty(
							MDCConstants.CLUSTERNAME) + MDCConstants.FORWARD_SLASH + MDCConstants.TSS,
					MDCConstants.LEADER,
					null);
			if (hostport != null || !Objects.isNull(taskscheduler)) {
				String currenttaskscheduler;
				// For docker container or kubernetes pods.
				if (hostport != null) {
					currenttaskscheduler = hostport;
				}
				// If not, obtain schedulers host port from zookeeper.
				else {
					var rand = new Random(System.currentTimeMillis());
					currenttaskscheduler = taskscheduler;
				}
				log.info("Using TaskScheduler host port: " + currenttaskscheduler);
				var mrjarpath = args[0];
				var ts = currenttaskscheduler.split(MDCConstants.UNDERSCORE);
				writeToTaskScheduler(ts, mrjarpath, args);
			}
		} catch (Exception ex) {
			log.error("Exception in submit Jar to Task Scheduler", ex);
		}
	}


	public static void writeToTaskScheduler(String[] ts, String mrjarpath, String[] args) {
		try (var s = new Socket(ts[0], Integer.parseInt(ts[1]));
				var is = s.getInputStream();
				var os = s.getOutputStream();
				var baos = new ByteArrayOutputStream();
				var fisjarpath = new FileInputStream(mrjarpath);
				var br = new BufferedReader(new InputStreamReader(is));) {
			int ch;
			while ((ch = fisjarpath.read()) != -1) {
				baos.write(ch);
			}
			// File bytes sent from localfile system to scheduler.
			writeDataStream(os, baos.toByteArray());
			// File name is sent to scheduler.
			writeDataStream(os, new File(mrjarpath).getName().getBytes());			
			if (args.length > 1) {
				for (var argsindex = 1; argsindex < args.length; argsindex++) {
					var arg = args[argsindex];
					log.info("Sending Arguments To Application: " + arg);
					writeDataStream(os, arg.getBytes());
				}
			}
			writeInt(os, -1);
			// Wait for tasks to get completed.
			while (true) {
				var messagetasksscheduler = (String) br.readLine();
				if(nonNull(messagetasksscheduler)) {
					log.info(messagetasksscheduler);
					if (messagetasksscheduler.trim().contains("quit")) {
						break;
					}
				}
			}
		} catch (Exception ex) {
			log.error("Exception in submit Jar to Task Scheduler", ex);
		}
	}

	/**
	 * Write integer value to scheduler 
	 * @param os
	 * @param value
	 * @throws Exception 
	 */
	public static void writeInt(OutputStream os, Integer value) throws Exception {
		byte[] bytes = Utils.getConfigForSerialization().asByteArray(value);
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeInt(bytes.length);
		dos.write(bytes);
		dos.flush();
	}

	/**
	 * Write bytes information to schedulers outputstream via kryo serializer.
	 * @param os
	 * @param outbyt
	 * @throws Exception 
	 */
	public static void writeDataStream(OutputStream os, byte[] outbyt) throws Exception {
		byte[] bytes = Utils.getConfigForSerialization().asByteArray(outbyt);
		DataOutputStream dos = new DataOutputStream(os);
		dos.writeInt(bytes.length);
		dos.write(bytes);
		dos.flush();
	}


}
