package com.github.mdc.tasks.scheduler;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Objects;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
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

public class ApplicationSubmitter {

	static Logger log = Logger.getLogger(ApplicationSubmitter.class);

	@SuppressWarnings({"unchecked"})
	public static void main(String[] args) throws Exception {

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

		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.BACKWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.BACKWARD_SLASH, MDCConstants.MDC_PROPERTIES);
		var cf = CuratorFrameworkFactory.newClient(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_HOSTPORT), 20000,
				50000, new RetryForever(2000));
		cf.start();
		var hostport = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HOSTPORT);
		var taskschedulers = (List<String>) ZookeeperOperations.nodesdata.invoke(cf,
				MDCConstants.ZK_BASE_PATH + MDCConstants.BACKWARD_SLASH + MDCConstants.TASKSCHEDULER, null, null);
		if (hostport != null || taskschedulers.size() > 0) {
			String currenttaskscheduler;
			if (hostport != null) {
				currenttaskscheduler = hostport;
			}
			else {
				currenttaskscheduler = taskschedulers.get(0);
			}
			var ts = currenttaskscheduler.split(MDCConstants.UNDERSCORE);
			try (var s = new Socket(ts[0], Integer.parseInt(ts[1]));
					var is = s.getInputStream();
					var os = s.getOutputStream();
					var fis = new FileInputStream(jarpath);
					var baos = new ByteArrayOutputStream();) {
				int ch;
				while ((ch = fis.read()) != -1) {
					baos.write(ch);
				}
				baos.flush();
				var kryo = Utils.getKryoNonDeflateSerializer();
				writeDataStream(os, baos.toByteArray());
				writeDataStream(os, new File(jarpath).getName().getBytes());
				if (!Objects.isNull(argue)) {
					for (var arg :argue) {
						writeDataStream(os, arg.getBytes());
					}
				}
				writeInt(os, -1);
				while (true) {
					var input = new Input(is);
					var messagetasksscheduler = (String) kryo.readObject(input, String.class);
					log.info(messagetasksscheduler);
					if ("quit".equals(messagetasksscheduler.trim())) {
						break;
					}
				}
			} catch (Exception ex) {
				log.error(MDCConstants.EMPTY, ex);
			}
		}
		cf.close();
	}

	public static void writeInt(OutputStream os, Integer value) throws Exception {
		var kryo = Utils.getKryoNonDeflateSerializer();
		var output = new Output(os);
		kryo.writeClassAndObject(output, value);
		output.flush();
	}

	public static void writeDataStream(OutputStream os, byte[] outbyt) throws Exception {
		var kryo = Utils.getKryoNonDeflateSerializer();
		var output = new Output(os);
		kryo.writeClassAndObject(output, outbyt);
		output.flush();
	}


}
