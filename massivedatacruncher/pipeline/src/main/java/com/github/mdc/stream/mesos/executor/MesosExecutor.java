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
package com.github.mdc.stream.mesos.executor;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;

import com.esotericsoftware.kryo.io.Input;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCMapReducePhaseClassLoader;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorMesos;

/**
 * 
 * @author Arun
 * Mesos executor driver.
 */
public class MesosExecutor implements Executor {

	String[] args;
	ExecutorService service;

	public MesosExecutor(String[] args) {
		this.args = args;
	}

	static Logger log = Logger.getLogger(MesosExecutor.class);

	@Override
	public void registered(ExecutorDriver driver, ExecutorInfo executorInfo, FrameworkInfo frameworkInfo,
			SlaveInfo slaveInfo) {
		service = Executors.newSingleThreadExecutor();
	}

	@Override
	public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {


	}

	@Override
	public void disconnected(ExecutorDriver driver) {


	}

	/**
	 * Get all the property files from the mesos property server running in master.
	 * @param httpurl
	 * @param propertyfiles
	 * @throws Throwable
	 */
	public void pullPropertiesMesosDistributor(String httpurl, String[] propertyfiles) throws Exception {
		for (var propertyfile :propertyfiles) {
			try (
			
			var istream = new URL(httpurl + MDCConstants.BACKWARD_SLASH + propertyfile).openStream();
			var fos = new FileOutputStream(propertyfile);) {
				IOUtils.copy(istream, fos);
			}
			catch (Exception ex) {
				log.error(MDCConstants.EMPTY, ex);
				throw ex;
			}
		}

	}

	/**
	 * Execute the tasks
	 */
	@Override
	public void launchTask(ExecutorDriver driver, TaskInfo task) {
		service.execute(() -> {
			try {

				Utils.loadPropertiesMesos(MDCConstants.MDC_PROPERTIES);
				var kryo = Utils.getKryoMesos();
				var bais = new ByteArrayInputStream(task.getData().toByteArray());
				var input = new Input(bais);
				//Get the jar bytes which contains the  MR job classes
				var jarandjobstage = (byte[]) kryo.readClassAndObject(input);
				var clsloader = MDCMapReducePhaseClassLoader
						.newInstance(jarandjobstage, getClass().getClassLoader());
				jarandjobstage = (byte[]) kryo.readClassAndObject(input);
				input.close();
				//Get the job stage object from jar.
				kryo.setClassLoader(clsloader);
				var jobstagestream = new Input(new ByteArrayInputStream(jarandjobstage));
				var job = (JobStage) kryo.readClassAndObject(jobstagestream);
				jobstagestream.close();
				//Initialize the mesos task executor.
				var mdstem = new StreamPipelineTaskExecutorMesos(job, driver, task.getTaskId());
				//Execute the tasks via run method.
				mdstem.call();
				log.debug(task.getTaskId() + " - Completed");

			}
			catch (Exception ex) {
				log.debug("Executing Tasks Failed: See cause below \n", ex);
			}
		});
	}

	@Override
	public void killTask(ExecutorDriver driver, TaskID taskId) {

	}

	@Override
	public void frameworkMessage(ExecutorDriver driver, byte[] data) {

	}

	@Override
	public void shutdown(ExecutorDriver driver) {
		if (service != null) {
			service.shutdown();
		}
	}

	@Override
	public void error(ExecutorDriver driver, String message) {

	}

	/**
	 * Start the mesos executor driver
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) {
		log.debug("Launching MassiveDataCruncher Mesos Executor");
		//Initialize the mesos executor driver with the 
		//mesos executor object inialized with the arguments.
		var driver = new MesosExecutorDriver(new MesosExecutor(args));
		//Run the driver.
		driver.run();
	}
}
