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
package com.github.mdc.stream.yarn.appmaster;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.springframework.yarn.integration.ip.mind.MindAppmasterService;
import org.springframework.yarn.integration.ip.mind.MindRpcMessageHolder;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;
import com.github.mdc.common.Task;
import com.github.mdc.common.Utils;

/**
 * 
 * @author Arun
 * Yarn application master service for stream pipelining jobs API. 
 */
public class StreamPipelineYarnAppmasterService extends MindAppmasterService {

	private static final Log log = LogFactory.getLog(StreamPipelineYarnAppmasterService.class);


	private StreamPipelineYarnAppmaster yarnAppMaster;
	
	public StreamPipelineYarnAppmasterService() {
		org.burningwave.core.assembler.StaticComponentContainer.Modules.exportAllToAll();
	}

	/**
	 * Retrieve the jobs request using MindApp Master Deserialiation 
	 * and return the response using the MindApp Master Serialization 
	 * classes configured in appmaster-context.xml.
	 */
	@Override
	protected MindRpcMessageHolder handleMindMessageInternal(MindRpcMessageHolder message) {
		var request = getConversionService().convert(message, BaseObject.class);
		var jobrequest = (JobRequest) request;
		log.debug("Request from container: " + jobrequest.getContainerid() + " " + jobrequest.getTimerequested());
		var response = handleJob(jobrequest);
		var mindrpcmessageholder = getConversionService().convert(response, MindRpcMessageHolder.class);
		log.debug("Response to container: " + response.getContainerid() + " :" + mindrpcmessageholder);
		return mindrpcmessageholder;
	}

	public StreamPipelineYarnAppmaster getYarnAppMaster() {
		return yarnAppMaster;
	}

	public void setYarnAppMaster(StreamPipelineYarnAppmaster yarnAppMaster) {
		this.yarnAppMaster = yarnAppMaster;
	}

	/**
	 * Handle the jobs request and return the jobs response.
	 * @param request
	 * @return
	 */
	private JobResponse handleJob(JobRequest request) {
		var response = new JobResponse(JobResponse.State.STANDBY, null);
		response.setResstate(JobResponse.State.STANDBY.name());
		response.setResmsg("" + request.getTimerequested());
		response.setContainerid(request.getContainerid());
		try {
			//Kryo for object serialization and deserialization.
			
			if (request.getJob() != null) {
				try (var input = new FSTObjectInput(new ByteArrayInputStream(request.getJob()), Utils.getConfigForSerialization());) {
					var object = input.readObject();
					var task = (Task) object;
					// Update statuses to App Master if job has been completed.
					if (request.getState().equals(JobRequest.State.JOBDONE)) {
						yarnAppMaster.reportJobStatus(task, true, request.getContainerid());
						return response;
					}
					// Update statuses to App Master if job has been Failed.
					else if (request.getState().equals(JobRequest.State.JOBFAILED)) {
						yarnAppMaster.reportJobStatus(task, false, request.getContainerid());
					} else if (request.getState().equals(JobRequest.State.RESPONSERECIEVED)) {
						yarnAppMaster.requestRecieved(task);
						return response;
					}
				} catch (Exception ex) {
					log.debug("Handle job request error, See cause below \n", ex);
				}
			}

			var job = yarnAppMaster.getTask(request.getContainerid());
			log.debug(request.getContainerid() + ": " + job);
			//Job is available
			if (job != null) {
				var baos = new ByteArrayOutputStream();
				var output = new FSTObjectOutput(baos, Utils.getConfigForSerialization());
				output.writeObject(job);
				output.flush();
				output.close();
				response.setJob(baos.toByteArray());
				if (job instanceof Map) {
					response.setState(JobResponse.State.STOREJOBSTAGE);
					response.setResstate(JobResponse.State.STOREJOBSTAGE.name());
				} else {
					response.setState(JobResponse.State.RUNJOB);
					response.setResstate(JobResponse.State.RUNJOB.name());
				}
			}
			//If there is no jobs to executor return the status for
			//container to DIE.
			else if (!yarnAppMaster.hasJobs()) {
				response.setState(JobResponse.State.DIE);
				response.setResstate(JobResponse.State.DIE.name());
			}
		}
		catch (Exception ex) {
			log.error("Handle job request error, See cause below \n", ex);
		}
		finally {
			log.debug("Response: state=" + response.getState() + " job=" + response.getJob());
		}
		return response;
	}


}
