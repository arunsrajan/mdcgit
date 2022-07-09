package com.github.mdc.tasks.scheduler;

import java.io.File;
import java.io.FileOutputStream;
import java.net.Socket;
import java.util.Arrays;

import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCMapReducePhaseClassLoader;
import com.github.mdc.common.Utils;

public class TaskScheduler implements Runnable {
	static Logger log = Logger.getLogger(TaskScheduler.class);
	CuratorFramework cf;
	byte[] mrjar;
	Socket tss;
	String[] args;
	String filename;

	public TaskScheduler(CuratorFramework cf, byte[] mrjar, String[] args, Socket tss, String filename) {
		this.cf = cf;
		this.mrjar = mrjar;
		this.args = args;
		this.tss = tss;
		this.filename = filename;
	}

	@Override
	public void run() {
		final ClassLoader loader = Thread.currentThread().getContextClassLoader();
		new File(MDCConstants.LOCAL_FS_APPJRPATH).mkdirs();
		try (var fos = new FileOutputStream(MDCConstants.LOCAL_FS_APPJRPATH + filename);) {
			fos.write(mrjar);
			var clsloader = MDCMapReducePhaseClassLoader.newInstance(mrjar, loader);
			Thread.currentThread().setContextClassLoader(clsloader);
			var kryo = Utils.getKryoNonDeflateSerializer();
			String[] argscopy;
			//Get the main class to execute.
			String mainclass;
			if (args == null) {
				argscopy = new String[]{};
				mainclass = "";
			} else {
				mainclass = args[0];
				argscopy = Arrays.copyOfRange(args, 1, args.length);
			}

			var main = Class.forName(mainclass, true, clsloader);
			var jc = JobConfigurationBuilder.newBuilder().build();
			jc.setMrjar(mrjar);
			var tssos = tss.getOutputStream();
			var output = new Output(tssos);
			jc.setOutput(output);
			var mrjob = (Application) main.getDeclaredConstructor().newInstance();
			mrjob.runMRJob(argscopy, jc);
			kryo.writeClassAndObject(output, "Successfully Completed executing the task " + mainclass);
			kryo.writeClassAndObject(output, "quit");
			output.close();
		} catch (Throwable ex) {
			log.error("Exception in loading class:", ex);
		} finally {
			Thread.currentThread().setContextClassLoader(loader);
			try {
				tss.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
}
