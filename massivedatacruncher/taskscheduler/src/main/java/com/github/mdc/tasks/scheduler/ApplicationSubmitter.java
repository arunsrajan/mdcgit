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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.log4j.Logger;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.Utils;
import com.github.mdc.common.ZookeeperOperations;
import com.github.mdc.tasks.scheduler.exception.ApplicationSubmitterException;

public class ApplicationSubmitter {

	static Logger log = Logger.getLogger(ApplicationSubmitter.class);

	@SuppressWarnings({"unchecked"})
	public static void main(String[] args) throws Exception {
		org.burningwave.core.assembler.StaticComponentContainer.Modules.exportAllToAll();
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		
		var options = new Options();

		options.addOption(MDCConstants.JAR, true, MDCConstants.MRJARREQUIRED);
		options.addOption(MDCConstants.ARGS, true, MDCConstants.ARGUEMENTSOPTIONAL);
		var parser = new DefaultParser();
		var cmd = parser.parse(options, args);

		String jarpath = null;
		String[] argue = null;
		if (cmd.hasOption(MDCConstants.JAR)) {
			jarpath = cmd.getOptionValue(MDCConstants.JAR);
		}
		else {
			var formatter = new HelpFormatter();
			formatter.printHelp(MDCConstants.ANTFORMATTER, options);
			return;
		}

		if (cmd.hasOption(MDCConstants.ARGS)) {
			argue = cmd.getOptionValue(MDCConstants.ARGS).split(" ");
		}

		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, MDCConstants.MDC_PROPERTIES);
		var cf = CuratorFrameworkFactory.newClient(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_HOSTPORT), 20000,
				50000, new RetryForever(2000));
		cf.start();
		var hostport = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HOSTPORT);
		var taskschedulers = (List<String>) ZookeeperOperations.nodesdata.invoke(cf,
				MDCConstants.ZK_BASE_PATH + MDCConstants.FORWARD_SLASH + MDCConstants.TASKSCHEDULER, null, null);
		if (hostport != null || !taskschedulers.isEmpty()) {
			String currenttaskscheduler;
			if (hostport != null) {
				currenttaskscheduler = hostport;
			}
			else {
				currenttaskscheduler = taskschedulers.get(0);
			}
			var ts = currenttaskscheduler.split(MDCConstants.UNDERSCORE);
			try (var s = Utils.createSSLSocket(ts[0], Integer.parseInt(ts[1]));
					var is = s.getInputStream();
					var os = s.getOutputStream();
					var fis = new FileInputStream(jarpath);
					var baos = new ByteArrayOutputStream();) {
				int ch;
				while ((ch = fis.read()) != -1) {
					baos.write(ch);
				}
				baos.flush();
				
				writeDataStream(os, baos.toByteArray());
				writeDataStream(os, new File(jarpath).getName().getBytes());
				if (!Objects.isNull(argue)) {
					for (var arg :argue) {
						writeDataStream(os, arg.getBytes());
					}
				}
				writeInt(os, -1);
				try (var br = new BufferedReader(new InputStreamReader(is));) {
					while (true) {					
						var messagetasksscheduler = (String) br.readLine();
						if ("quit".equals(messagetasksscheduler.trim())) {
							break;
						}
						log.info(messagetasksscheduler);
					}
				}
			} catch (Exception ex) {
				log.error(MDCConstants.EMPTY, ex);
			}
		}
		cf.close();
	}

	public static void writeInt(OutputStream os, Integer value) throws ApplicationSubmitterException {
		try {
			
			byte[] bytes = Utils.getConfigForSerialization().asByteArray(value);
			DataOutputStream dos = new DataOutputStream(os);
			dos.writeInt(bytes.length);
			dos.write(bytes);
			dos.flush();
		} catch(Exception exception){
			throw new ApplicationSubmitterException(exception, "Unable to write Integer value to the output stream");
		}
	}

	public static void writeDataStream(OutputStream os, byte[] outbyt) throws ApplicationSubmitterException {
		try {
			byte[] bytes = Utils.getConfigForSerialization().asByteArray(outbyt);
			DataOutputStream dos = new DataOutputStream(os);
			dos.writeInt(bytes.length);
			dos.write(bytes);
			dos.flush();
		} catch(Exception exception){
			throw new ApplicationSubmitterException(exception, "Unable to write Integer value to the output stream");
		}
	}


}
