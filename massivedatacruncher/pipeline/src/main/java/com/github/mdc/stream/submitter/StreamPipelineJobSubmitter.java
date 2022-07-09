package com.github.mdc.stream.submitter;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Objects;
import java.util.Random;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
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
		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.BACKWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.BACKWARD_SLASH, MDCConstants.MDC_PROPERTIES);
		try (var cf = CuratorFrameworkFactory.newClient(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_HOSTPORT),
				20000, 50000, new RetryForever(2000));) {
			cf.start();
			var hostport = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOSTPORT);
			var taskscheduler = (String) ZookeeperOperations.nodedata.invoke(cf,
					MDCConstants.BACKWARD_SLASH + MDCProperties.get().getProperty(
							MDCConstants.CLUSTERNAME) + MDCConstants.BACKWARD_SLASH + MDCConstants.TSS,
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
				var input = new Input(is);) {
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
			var kryo = Utils.getKryoNonDeflateSerializer();
			// Wait for tasks to get completed.
			while (true) {
				var messagetasksscheduler = (String) kryo.readObject(input, String.class);
				log.info(messagetasksscheduler);
				if ("quit".equals(messagetasksscheduler.trim())) {
					break;
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
	public static void writeInt(OutputStream os, Integer value) {
		var kryo = Utils.getKryoNonDeflateSerializer();
		var output = new Output(os);
		kryo.writeClassAndObject(output, value);
		output.flush();
	}

	/**
	 * Write bytes information to schedulers outputstream via kryo serializer.
	 * @param os
	 * @param outbyt
	 * @throws Exception
	 */
	public static void writeDataStream(OutputStream os, byte[] outbyt) {
		var kryo = Utils.getKryoNonDeflateSerializer();
		var output = new Output(os);
		kryo.writeClassAndObject(output, outbyt);
		output.flush();
	}


}
