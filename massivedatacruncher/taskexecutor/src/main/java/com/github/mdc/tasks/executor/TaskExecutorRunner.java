/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.mdc.tasks.executor;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectInput;
import org.slf4j.LoggerFactory;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.CacheUtils;
import com.github.mdc.common.HeartBeatStream;
import com.github.mdc.common.JobApp;
import com.github.mdc.common.JobStage;
import com.github.mdc.common.LoadJar;
import com.github.mdc.common.MDCCache;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCMapReducePhaseClassLoader;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.ServerUtils;
import com.github.mdc.common.StreamDataCruncher;
import com.github.mdc.common.Task;
import com.github.mdc.common.TaskExecutorShutdown;
import com.github.mdc.common.Utils;
import com.github.mdc.common.WebResourcesServlet;
import com.github.mdc.common.ZookeeperOperations;
import com.github.mdc.tasks.executor.web.NodeWebServlet;
import com.github.mdc.tasks.executor.web.ResourcesMetricsServlet;

public class TaskExecutorRunner implements TaskExecutorRunnerMBean {

  static org.slf4j.Logger log = LoggerFactory.getLogger(TaskExecutorRunner.class);
  Map<String, Object> apptaskexecutormap = new ConcurrentHashMap<>();
  Map<String, Object> jobstageexecutormap = new ConcurrentHashMap<>();
  ConcurrentMap<String, OutputStream> resultstream = new ConcurrentHashMap<>();
  Map<String, HeartBeatStream> containeridhbss = new ConcurrentHashMap<>();
  Map<String, Map<String, Object>> jobidstageidexecutormap = new ConcurrentHashMap<>();
  Map<String, JobStage> jobidstageidjobstagemap = new ConcurrentHashMap<>();
  Queue<Object> taskqueue = new LinkedBlockingQueue<Object>();
  CuratorFramework cf;
  static ExecutorService es;
  static CountDownLatch shutdown = new CountDownLatch(1);

  public static void main(String[] args) throws Exception {
    org.burningwave.core.assembler.StaticComponentContainer.Modules.exportAllToAll();
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    if (args == null || args.length != 2) {
      log.debug("Args" + args);
      if (args != null) {
        log.debug("Args Not of Length 2!=" + args.length);
        for (var arg : args) {
          log.debug(arg);
        }
      }
      System.exit(1);
    }
    if (args.length == 2) {
      log.debug("Args = ");
      for (var arg : args) {
        log.debug(arg);
      }
    }
    if (args[0].equals(MDCConstants.TEPROPLOADDISTROCONFIG)) {
      Utils
          .loadLog4JSystemProperties(
              MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
                  + MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH,
              MDCConstants.MDC_PROPERTIES);
    }
    ByteBufferPoolDirect.init();
    CacheUtils.initCache();
    int numberofprocessors = Runtime.getRuntime().availableProcessors();
    es = new ForkJoinPool(numberofprocessors * 2);
    var mdted = new TaskExecutorRunner();
    mdted.init();
    mdted.start();
    log.info("TaskExecuterRunner evoked at port....."
        + System.getProperty(MDCConstants.TASKEXECUTOR_PORT));
    log.info("Reckoning stoppage holder...");
    shutdown.await();
    try {
      log.info("Ceasing the connections...");
      mdted.destroy();
      ByteBufferPoolDirect.destroy();
      log.info("Freed the assets...");
      Runtime.getRuntime().halt(0);
    } catch (Exception e) {
      log.error("", e);
    }

  }

  @SuppressWarnings("unchecked")
  @Override
  public void init() throws Exception {
    cf = CuratorFrameworkFactory.newClient(
        MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_HOSTPORT), 20000, 50000,
        new RetryForever(
            Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_RETRYDELAY))));
    cf.start();
    ZookeeperOperations.addconnectionstate.addConnectionStateListener(cf,
        (CuratorFramework cf, ConnectionState cs) -> {
          if (cs == ConnectionState.RECONNECTED) {
            var nodedata = NetworkUtil
                .getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST))
                + MDCConstants.UNDERSCORE
                + MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PORT);
            var nodesdata = (List<String>) ZookeeperOperations.nodesdata.invoke(cf,
                MDCConstants.ZK_BASE_PATH + MDCConstants.FORWARD_SLASH + MDCConstants.TASKEXECUTOR,
                null, null);
            if (!nodesdata.contains(nodedata)) {
              ZookeeperOperations.ephemeralSequentialCreate
                  .invoke(cf,
                      MDCConstants.ZK_BASE_PATH + MDCConstants.FORWARD_SLASH
                          + MDCConstants.TASKEXECUTOR,
                      MDCConstants.TE + MDCConstants.HYPHEN, nodedata);
            }
          }
        });

    ZookeeperOperations.ephemeralSequentialCreate.invoke(cf,
        MDCConstants.ZK_BASE_PATH + MDCConstants.FORWARD_SLASH + MDCConstants.TASKEXECUTOR,
        MDCConstants.TE + MDCConstants.HYPHEN,
        NetworkUtil
            .getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST))
            + MDCConstants.UNDERSCORE
            + MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PORT));

  }

  ClassLoader cl;
  static Registry server = null;

  @SuppressWarnings({})
  @Override
  public void start() throws Exception {
    var port = Integer.parseInt(System.getProperty(MDCConstants.TASKEXECUTOR_PORT));
    log.info("TaskExecutor Port: " + port);
    var su = new ServerUtils();
    log.info("Initializing Server at: {}", port);
    su.init(port + MDCConstants.PORT_OFFSET,
        new NodeWebServlet(new ConcurrentHashMap<String, Map<String, Process>>()),
        MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX, new WebResourcesServlet(),
        MDCConstants.FORWARD_SLASH + MDCConstants.RESOURCES + MDCConstants.FORWARD_SLASH
            + MDCConstants.ASTERIX,
        new ResourcesMetricsServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.DATA
            + MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX);
    log.info("Jetty Server initialized at: {}", port);
    su.start();
    log.info("Jetty Server started and listening: {}", port);
    var configuration = new Configuration();

    var inmemorycache = MDCCache.get();
    cl = TaskExecutorRunner.class.getClassLoader();
    dataCruncher = new StreamDataCruncher() {
      public Object postObject(Object deserobj) throws RemoteException {
        Task task = new Task();
        try {
          log.info("Deserialized object: " + deserobj);
          if (deserobj instanceof byte[] bytes) {
            FSTConfiguration conf = Utils.getConfigForSerialization();
            conf.setClassLoader(cl);
            FSTObjectInput.ConditionalCallback conditionalCallback =
                new FSTObjectInput.ConditionalCallback() {
                  @Override
                  public boolean shouldSkip(Object halfDecoded, int streamPosition, Field field) {
                    log.info("Skip half decoded: " + halfDecoded);
                    return true;
                  }
                };
            try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                FSTObjectInput fstin = new FSTObjectInput(bais, conf);) {
              fstin.setConditionalCallback(conditionalCallback);
              deserobj = fstin.readObject();
            }
          }
          if (deserobj instanceof TaskExecutorShutdown) {
            shutdown.countDown();
          } else if (deserobj instanceof LoadJar loadjar) {
            log.info("Unpacking jars: " + loadjar.getMrjar());
            cl = MDCMapReducePhaseClassLoader.newInstance(loadjar.getMrjar(), cl);
            return MDCConstants.JARLOADED;
          } else if (deserobj instanceof JobApp jobapp) {
            var containerid = (String) jobapp.getContainerid();
            if (Objects.isNull(containeridhbss.get(containerid))) {
              HeartBeatStream hbss = new HeartBeatStream();
              var teport =
                  Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_PORT));
              var pingdelay = Integer
                  .parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PINGDELAY));
              var host = NetworkUtil.getNetworkAddress(
                  MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST));
              log.info("Kickoff hearbeat for chamber id: {} with host {} and port {}", containerid,
                  host, teport);
              hbss.init(0, teport, host, 0, pingdelay, containerid);
              hbss.ping();
              containeridhbss.put(containerid, hbss);
            }
          } else if (!Objects.isNull(deserobj)) {
            TaskExecutor taskexecutor = new TaskExecutor(cl, port, es, configuration,
                apptaskexecutormap, jobstageexecutormap, resultstream, inmemorycache, deserobj,
                containeridhbss, jobidstageidexecutormap, task, jobidstageidjobstagemap);
            return taskexecutor.call();
          }
        } catch (InterruptedException e) {
          log.warn("Interrupted!", e);
          // Restore interrupted state...
          Thread.currentThread().interrupt();
        } catch (Exception ex) {
          log.error(MDCConstants.EMPTY, ex);
          Utils.getStackTrace(ex, task);
        }
        return task;
      }
    };
    log.info("Getting RPC Registry for port: {}", port);
    server = Utils.getRPCRegistry(port, dataCruncher);
    log.info("RPC Registry for port: {} Obtained", port);
  }

  static StreamDataCruncher stub = null;
  static StreamDataCruncher dataCruncher = null;

  @Override
  public void destroy() throws Exception {
    containeridhbss.keySet().stream().filter(key -> !Objects.isNull(containeridhbss.get(key)))
        .forEach(key -> {
          try {
            containeridhbss.remove(key).close();
          } catch (Exception e) {
          }
        });
    if (cf != null) {
      cf.close();
    }
    if (es != null) {
      es.shutdownNow();
      es.awaitTermination(1, TimeUnit.SECONDS);
    }
  }

  static class ExecutorsFutureTask {
    @SuppressWarnings("rawtypes")
    Future future;
    ExecutorService estask;
  }

}
