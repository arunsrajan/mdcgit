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

public class MassiveDataTaskScheduler implements Runnable {
	static Logger log = Logger.getLogger(MassiveDataTaskScheduler.class);
	CuratorFramework cf;
	byte[] mrjar;
	Socket tss;
	String[] args;
	String filename;
	public MassiveDataTaskScheduler(CuratorFramework cf, byte[] mrjar, String[] args, Socket tss, String filename) {
		this.cf = cf;
		this.mrjar = mrjar;
		this.args = args;
		this.tss = tss;
		this.filename = filename;
	}

	@Override
	public void run() {
		final ClassLoader loader = Thread.currentThread().getContextClassLoader();
		try {
			new File(MDCConstants.LOCAL_FS_APPJRPATH).mkdirs();
			var fos = new FileOutputStream(MDCConstants.LOCAL_FS_APPJRPATH+filename);
			fos.write(mrjar);
			fos.close();
			var clsloader = MDCMapReducePhaseClassLoader.newInstance(mrjar, loader);
			Thread.currentThread().setContextClassLoader(clsloader);
			var kryo = Utils.getKryoNonDeflateSerializer();

			//Get the main class to execute.
			var mainclass = args[0];
			var main = Class.forName(mainclass,true,clsloader);
			if(args==null) {
				args = new String[] {};
			} else {
				args = Arrays.copyOfRange(args, 1, args.length);
			}
			var jc = JobConfigurationBuilder.newBuilder().build();
			jc.setMrjar(mrjar);
			var tssos = tss.getOutputStream();
			var output = new Output(tssos);
			jc.setOutput(output);
			var mrjob = (MRJob) main.newInstance();
			mrjob.runMRJob(args, jc);
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
