/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.mdc.stream.scheduler;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.Callable;
import org.apache.log4j.Logger;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.StreamDataCruncher;
import com.github.mdc.common.StreamPipelineTaskSubmitterMBean;
import com.github.mdc.common.Task;

/**
 * 
 * @author Arun The task scheduler thread for submitting tasks using jobstage object.
 */
public class StreamPipelineTaskSubmitter
    implements StreamPipelineTaskSubmitterMBean, Callable<Object> {

  static Logger log = Logger.getLogger(StreamPipelineTaskSubmitter.class);
  private int level;
  private Task task;
  private String hp;
  private boolean completedexecution;
  private boolean resultobtainedte;


  public StreamPipelineTaskSubmitter() {}


  public boolean isCompletedexecution() {
    return completedexecution;
  }


  public void setCompletedexecution(boolean completedexecution) {
    this.completedexecution = completedexecution;
  }


  public StreamPipelineTaskSubmitter(Task task, String hp) {
    this.task = task;
    this.hp = hp;
  }

  public Task getTask() {
    return task;
  }

  public void setTask(Task task) {
    this.task = task;
  }

  /**
   * Submit the job stages to task exectuors.
   */
  @Override
  public Object call() throws Exception {
    try {
      String hostport[] = hp.split(MDCConstants.UNDERSCORE);
      Registry registry = LocateRegistry.getRegistry(hostport[0], Integer.parseInt(hostport[1]));
      StreamDataCruncher sdc = (StreamDataCruncher) registry.lookup(MDCConstants.BINDTESTUB);
      return sdc.postObject(task);
    } catch (Exception ex) {
      log.error("Unable to connect and submit tasks to executor: ", ex);
      throw ex;
    }
  }


  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((task == null) ? 0 : task.hashCode());
    return result;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    StreamPipelineTaskSubmitter other = (StreamPipelineTaskSubmitter) obj;
    if (hp == null) {
      if (other.hp != null) {
        return false;
      }
    } else if (!hp.equals(other.hp)) {
      return false;
    }
    if (task == null) {
      if (other.task != null) {
        return false;
      }
    } else if (!task.equals(other.task)) {
      return false;
    }
    return true;
  }


  @Override
  public String toString() {
    return "StreamPipelineTaskSubmitter [task=" + task + ", hp=" + hp + ", completedexecution="
        + completedexecution + "]";
  }


  @Override
  public void setHostPort(String hp) {
    this.hp = hp;

  }

  @Override
  public String getHostPort() {
    return hp;
  }


  public int getLevel() {
    return level;
  }


  public void setLevel(int level) {
    this.level = level;
  }


  public boolean isResultobtainedte() {
    return resultobtainedte;
  }


  public void setResultobtainedte(boolean resultobtainedte) {
    this.resultobtainedte = resultobtainedte;
  }


}
