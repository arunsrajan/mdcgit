package com.github.mdc.stream.yarn.container;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.yarn.integration.container.AbstractIntegrationYarnContainer;
import org.springframework.yarn.integration.ip.mind.MindAppmasterServiceClient;

import com.esotericsoftware.kryo.io.Input;
import com.github.mdc.common.ByteBufferPool;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.Task;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.executors.StreamPipelineTaskExecutorYarn;
import com.github.mdc.stream.yarn.appmaster.JobRequest;
import com.github.mdc.stream.yarn.appmaster.JobResponse;

/**
 * 
 * @author Arun
 * The yarn container executor for to process Map Reduce pipelining API.  
 */
public class StreamPipelineYarnContainer extends AbstractIntegrationYarnContainer {

	private Map<String, String> containerprops;
	private ExecutorService executor;
	private static final Log log = LogFactory.getLog(StreamPipelineYarnContainer.class);
	private Map<String, JobStage> jsidjsmap;

	/**
	 * Pull the Job to perform MR operation execution requesting 
	 * the Yarn App Master Service. The various Yarn operation What operation
	 * to execute i.e WHATTODO,JOBDONE,JOBFAILED. The various operations response from Yarn App master are
	 * STANDBY,RUNJOB or DIE.
	 */
	@Override
	protected void runInternal() {
		Task task;
		JobRequest request;
		byte[] job = null;
		var containerid = getEnvironment().get(MDCConstants.SHDP_CONTAINERID);
		MindAppmasterServiceClient client = null;
		executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		try {
			var prop = new Properties();
			prop.putAll(System.getProperties());
			MDCProperties.put(prop);
			ByteBufferPoolDirect.init();
			ByteBufferPool.init(3);
			while (true) {
				request = new JobRequest();
				request.setState(JobRequest.State.WHATTODO);
				request.setContainerid(containerid);
				request.setTimerequested(System.currentTimeMillis());
				log.debug(request.getTimerequested());
				client = (MindAppmasterServiceClient) getIntegrationServiceClient();
				var response = (JobResponse) client.doMindRequest(request);
				log.debug(containerid + ": Response containerid: " + response);
				if (response == null) {
					sleep(1);
					continue;
				}
				if (response.getJob() != null) {
					request = new JobRequest();
					request.setState(JobRequest.State.RESPONSERECIEVED);
					request.setContainerid(containerid);
					request.setTimerequested(System.currentTimeMillis());
					request.setJob(response.getJob());
					client.doMindRequest(request);
				}
				log.debug(containerid + ": Response containerid: " + response.getContainerid());
				log.debug(containerid + ": Response State: " + response.getState() + " " + response.getResmsg());
				if (response.getState().equals(JobResponse.State.STANDBY)) {
					sleep(1);
					continue;
				}
				else if (response.getState().equals(JobResponse.State.STOREJOBSTAGE)) {
					job = response.getJob();
					var kryo = Utils.getKryoNonDeflateSerializer();
					var input = new Input(new ByteArrayInputStream(job));
					var object = kryo.readClassAndObject(input);
					this.jsidjsmap = (Map<String, JobStage>) object;
					sleep(1);
				}
				else if (response.getState().equals(JobResponse.State.RUNJOB)) {
					log.debug(containerid + ": Environment " + getEnvironment());
					job = response.getJob();
					var kryo = Utils.getKryoNonDeflateSerializer();
					var input = new Input(new ByteArrayInputStream(job));
					var object = kryo.readClassAndObject(input);
					task = (Task) object;
					System.setProperty(MDCConstants.HDFSNAMENODEURL, containerprops.get(MDCConstants.HDFSNAMENODEURL));
					prop.putAll(containerprops);
					MDCProperties.put(prop);
					var yarnexecutor = new StreamPipelineTaskExecutorYarn( containerprops.get(MDCConstants.HDFSNAMENODEURL), jsidjsmap.get(task.jobid + task.stageid));
					yarnexecutor.setTask(task);
					yarnexecutor.setExecutor(executor);
					yarnexecutor.call();
					request = new JobRequest();
					request.setState(JobRequest.State.JOBDONE);
					request.setJob(job);
					request.setContainerid(containerid);
					response = (JobResponse) client.doMindRequest(request);
					log.debug(containerid + ": Task Completed=" + task);
					sleep(1);
				}
				else if (response.getState().equals(JobResponse.State.DIE)) {
					log.debug(containerid + ": Container dies: " + response.getState());
					break;
				}
				log.debug(containerid + ": Response state=" + response.getState());

			}
			log.debug(containerid + ": Completed Job Exiting with status 0...");
			ByteBufferPoolDirect.get().close();
			shutdownExecutor();
			System.exit(0);
		}
		catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		}
		catch (Exception ex) {
			request = new JobRequest();
			request.setState(JobRequest.State.JOBFAILED);
			request.setJob(job);
			if (client != null) {
				var response = (JobResponse) client.doMindRequest(request);
				log.debug("Job Completion Error..." + response.getState() + "..., See cause below \n", ex);
			}
			ByteBufferPoolDirect.get().close();
			try {
				shutdownExecutor();
			} catch (InterruptedException e) {
				log.warn("Interrupted!", e);
				// Restore interrupted state...
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				log.error("", e);
			}
			System.exit(-1);
		}
	}

	public void shutdownExecutor() throws InterruptedException {
		if (executor != null) {
			executor.shutdownNow();
			executor.awaitTermination(1, TimeUnit.SECONDS);
		}
	}

	public Map<String, String> getContainerprops() {
		return containerprops;
	}

	public void setContainerprops(Map<String, String> containerprops) {
		this.containerprops = containerprops;
	}

	private static void sleep(int seconds) {
		try {
			Thread.sleep(1000l * seconds);
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.debug("Delay error, See cause below \n", ex);
		}
	}

}
