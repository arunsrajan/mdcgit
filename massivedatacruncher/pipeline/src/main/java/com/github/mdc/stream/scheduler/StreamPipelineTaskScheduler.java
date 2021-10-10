package com.github.mdc.stream.scheduler;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.io.Output;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCMapReducePhaseClassLoader;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Task;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.Pipeline;

/**
 * 
 * @author Arun
 * The stream pipelining API task scheduler thread to execut the MR jar file.
 */
public class StreamPipelineTaskScheduler implements Runnable{
	private static Logger log = Logger.getLogger(StreamPipelineTaskScheduler.class);
	private byte[] mrjar; 
	private Socket tss;
	private String[] args;
	String filename;
	public StreamPipelineTaskScheduler(CuratorFramework cf, String filename, byte[] mrjar,String[] args,
			 Socket tss) {
		this.mrjar = mrjar;
		this.args = args;
		this.tss = tss;
		this.filename = filename;
	}

	@Override
	public void run() {
		var kryo = Utils.getKryoNonDeflateSerializer();
		var message = "";
		try {
			//ClassLoader to load the jar file.
			ClassLoader ctxcl=Thread.currentThread().getContextClassLoader();
			var clsloader = MDCMapReducePhaseClassLoader.newInstance(mrjar, ctxcl);
			Thread.currentThread().setContextClassLoader(clsloader);
			var ismesos = Boolean.parseBoolean(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_ISMESOS));
			var isyarn = Boolean.parseBoolean(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_ISYARN));
			//If not mesos and yarn load the jar by invoking socket of the task executors.
			if(ismesos || isyarn) {
				new File(MDCConstants.LOCAL_FS_APPJRPATH).mkdirs();
				var fos = new FileOutputStream(MDCConstants.LOCAL_FS_APPJRPATH+filename);
				fos.write(mrjar);
				fos.close();
				
			}
			//Get the main class to execute.
			var mainclass = args[0];
			var main = Class.forName(mainclass,true,clsloader);
			Thread.currentThread().setContextClassLoader(ctxcl);
			if(args==null) {
				args = new String[] {};
			} else {
				args = Arrays.copyOfRange(args, 1, args.length);
			}
			//Invoke the runPipeline method via reflection.
			var pipelineconfig = new PipelineConfig();
			pipelineconfig.setJar(mrjar);
			pipelineconfig.setKryoOutput(new Output(tss.getOutputStream()));
			var pipeline = (Pipeline) main.newInstance();
			pipelineconfig.setJobname(main.getSimpleName());
			pipeline.runPipeline(args, pipelineconfig);
			message = "Successfully Completed executing the Job from main class "+mainclass;
			Utils.writeKryoOutput(kryo, new Output(tss.getOutputStream()), message);
		}
		catch(Throwable ex) {
			log.error("Job execution Error, See cause below \n",ex);
			try (var baos = new ByteArrayOutputStream();) {
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
				Utils.writeKryoOutput(kryo, new Output(tss.getOutputStream()), new String(baos.toByteArray()));
			} catch (Exception e) {
				log.error("Message Send Failed for Task Failed: ", e);
			}
		}
		finally {
			try {
				Utils.writeKryoOutput(kryo, new Output(tss.getOutputStream()), "quit");
				tss.close();
			} catch (Exception ex) {
				log.error("Socket Stream close error, See cause below \n",ex);
			}
		}
		
	}
}
