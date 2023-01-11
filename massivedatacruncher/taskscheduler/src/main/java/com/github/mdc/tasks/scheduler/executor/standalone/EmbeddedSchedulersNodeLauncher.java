package com.github.mdc.tasks.scheduler.executor.standalone;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.slf4j.LoggerFactory;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.CacheUtils;
import com.github.mdc.common.HeartBeat;
import com.github.mdc.common.HeartBeatStream;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.NetworkUtil;
import com.github.mdc.common.ServerUtils;
import com.github.mdc.common.StreamDataCruncher;
import com.github.mdc.common.TaskSchedulerWebServlet;
import com.github.mdc.common.Utils;
import com.github.mdc.common.WebResourcesServlet;
import com.github.mdc.common.ZookeeperOperations;
import com.github.mdc.stream.scheduler.StreamPipelineTaskScheduler;
import com.github.mdc.tasks.executor.NodeRunner;
import com.github.mdc.tasks.executor.web.NodeWebServlet;
import com.github.mdc.tasks.executor.web.ResourcesMetricsServlet;
import com.github.mdc.tasks.scheduler.TaskScheduler;

public class EmbeddedSchedulersNodeLauncher {
  static org.slf4j.Logger log = LoggerFactory.getLogger(EmbeddedSchedulersNodeLauncher.class);

  public static final String STOPPINGANDCLOSECONNECTION =
      "Stopping and closes all the connections...";

  public static void main(String[] args) throws Exception {
    org.burningwave.core.assembler.StaticComponentContainer.Modules.exportAllToAll();
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
        + MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH,
        MDCConstants.MDC_PROPERTIES);
    var cdl = new CountDownLatch(3);
    var clientport = Integer
        .parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_STANDALONE_CLIENTPORT,
            MDCConstants.ZOOKEEPER_STANDALONE_CLIENTPORT_DEFAULT));
    var numconnections = Integer
        .parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_STANDALONE_NUMCONNECTIONS,
            MDCConstants.ZOOKEEPER_STANDALONE_NUMCONNECTIONS_DEFAULT));
    var ticktime =
        Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_STANDALONE_TICKTIME,
            MDCConstants.ZOOKEEPER_STANDALONE_TICKTIME_DEFAULT));
    ServerCnxnFactory scf = null;
    try (var cf = CuratorFrameworkFactory.newClient(
        MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_HOSTPORT), 20000, 50000,
        new RetryForever(Integer
            .parseInt(MDCProperties.get().getProperty(MDCConstants.ZOOKEEPER_RETRYDELAY))));) {
      scf = Utils.startZookeeperServer(clientport, numconnections, ticktime);
      cf.start();
      cf.blockUntilConnected();
      ByteBufferPoolDirect.init();
      CacheUtils.initBlockMetadataCache();
      startTaskScheduler(cf, cdl);
      startTaskSchedulerStream(cf, cdl);
      startContainerLauncher(cdl);
      String nodeport = MDCProperties.get().getProperty(MDCConstants.NODE_PORT);
      String streamport = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT);
      String streamwebport =
          MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_WEB_PORT);
      String mrport = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT);
      String mrwebport = MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_WEB_PORT);
      log.info(
          "Program evoked in the port Stream[port={},webport={}] MapReduce[port={},webport={}] Node[port={}]",
          streamport, streamwebport, mrport, mrwebport, nodeport);
      cdl.await();
    } catch (InterruptedException e) {
      log.warn("Interrupted!", e);
      // Restore interrupted state...
      Thread.currentThread().interrupt();
    } catch (Exception ex) {
      log.error(MDCConstants.EMPTY, ex);
    }
    if (!Objects.isNull(scf)) {
      scf.closeAll();
    }
    Runtime.getRuntime().halt(0);
  }

  static Registry server = null;

  @SuppressWarnings("resource")
  public static void startContainerLauncher(CountDownLatch cdl) {
    HeartBeatStream hbss = new HeartBeatStream();
    try {
      var port = Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.NODE_PORT));
      var pingdelay =
          Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PINGDELAY));
      var host = NetworkUtil
          .getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKEXECUTOR_HOST));
      hbss.init(0, port, host, 0, pingdelay, "");
      var hb = new HeartBeat();
      hb.init(0, port, host, 0, pingdelay, "");
      hbss.ping();
      var escontainer = Executors.newWorkStealingPool();

      var hdfs =
          FileSystem.get(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
              new Configuration());
      var containerprocesses = new ConcurrentHashMap<String, Map<String, Process>>();
      var containeridthreads = new ConcurrentHashMap<String, Map<String, List<Thread>>>();
      var containeridports = new ConcurrentHashMap<String, List<Integer>>();
      var su = new ServerUtils();
      su.init(port + MDCConstants.PORT_OFFSET, new NodeWebServlet(containerprocesses),
          MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX, new WebResourcesServlet(),
          MDCConstants.FORWARD_SLASH + MDCConstants.RESOURCES + MDCConstants.FORWARD_SLASH
              + MDCConstants.ASTERIX,
          new ResourcesMetricsServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.DATA
              + MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX);
      su.start();
      server = LocateRegistry.createRegistry(port);
      datacruncher = new StreamDataCruncher() {
        public Object postObject(Object object) {
          try {
            var container = new NodeRunner(MDCConstants.PROPLOADERCONFIGFOLDER, containerprocesses,
                hdfs, containeridthreads, containeridports, object);
            Future<Object> containerallocated = escontainer.submit(container);
            Object retobj = containerallocated.get();
            log.info("Node processor refined the {} with status {} ", object, retobj);
            return retobj;
          } catch (InterruptedException e) {
            log.warn("Interrupted!", e);
            // Restore interrupted state...
            Thread.currentThread().interrupt();
          } catch (Exception e) {
            log.error(MDCConstants.EMPTY, e);
          }
          return null;
        }
      };
      stub = (StreamDataCruncher) UnicastRemoteObject.exportObject(datacruncher, 0);
      server.rebind(MDCConstants.BINDTESTUB, stub);
      log.debug("NodeLauncher started at port....."
          + MDCProperties.get().getProperty(MDCConstants.NODE_PORT));
      log.debug("Adding Shutdown Hook...");
      Utils.addShutdownHook(() -> {
        try {
          containerprocesses
              .keySet().stream().map(containerprocesses::get).flatMap(mapproc -> mapproc.keySet()
                  .stream().map(key -> mapproc.get(key)).collect(Collectors.toList()).stream())
              .forEach(proc -> {
                log.debug("Destroying the Container Process: " + proc);
                proc.destroy();
              });
          log.debug(STOPPINGANDCLOSECONNECTION);
          log.debug("Destroying...");
          if (!Objects.isNull(hbss)) {
            hbss.close();
          }
          if (!Objects.isNull(hdfs)) {
            hdfs.close();
          }
          cdl.countDown();
        } catch (Exception e) {
          log.debug("", e);
        }
      });
    } catch (Exception ex) {
      log.error("Unable to start Node Manager due to ", ex);
    }
  }

  static StreamDataCruncher stub = null;
  static StreamDataCruncher datacruncher = null;

  public static void startTaskSchedulerStream(CuratorFramework cf, CountDownLatch cdl)
      throws Exception {
    var esstream = Executors.newFixedThreadPool(1);
    var es = Executors.newWorkStealingPool();
    var su = new ServerUtils();
    su.init(
        Integer
            .parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_WEB_PORT)),
        new TaskSchedulerWebServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX,
        new WebResourcesServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.RESOURCES
            + MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX);
    su.start();
    if (!(boolean) ZookeeperOperations.checkexists.invoke(cf,
        MDCConstants.FORWARD_SLASH + MDCProperties.get()
            .getProperty(MDCConstants.CLUSTERNAME) + MDCConstants.FORWARD_SLASH + MDCConstants.TSS,
        MDCConstants.LEADER,
        NetworkUtil.getNetworkAddress(
            MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST))
            + MDCConstants.UNDERSCORE
            + MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT))) {
      ZookeeperOperations.persistentCreate.invoke(cf,
          MDCConstants.FORWARD_SLASH + MDCProperties.get().getProperty(
              MDCConstants.CLUSTERNAME) + MDCConstants.FORWARD_SLASH + MDCConstants.TSS,
          MDCConstants.LEADER,
          NetworkUtil.getNetworkAddress(
              MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST))
              + MDCConstants.UNDERSCORE
              + MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT));
    } else {
      ZookeeperOperations.writedata.invoke(cf,
          MDCConstants.FORWARD_SLASH
              + MDCProperties.get()
                  .getProperty(MDCConstants.CLUSTERNAME)
              + MDCConstants.FORWARD_SLASH + MDCConstants.TSS + MDCConstants.FORWARD_SLASH
              + MDCConstants.LEADER,
          MDCConstants.EMPTY,
          NetworkUtil.getNetworkAddress(
              MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST))
              + MDCConstants.UNDERSCORE
              + MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT));
    }
    var hbss = new HeartBeatStream();
    hbss.init(
        Integer.parseInt(
            MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY)),
        Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT)),
        NetworkUtil.getNetworkAddress(
            MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST)),
        Integer.parseInt(
            MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_INITIALDELAY)),
        Integer.parseInt(
            MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PINGDELAY)),
        "");
    // Start Resources gathering via heart beat resources
    // status update.
    hbss.start();

    // Execute when request arrives.
    esstream.execute(() -> {
      try (var ss = new ServerSocket(Integer
          .parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_PORT)));) {
        while (true) {
          try {
            var s = ss.accept();
            var bytesl = new ArrayList<byte[]>();
            var in = new DataInputStream(s.getInputStream());
            var config = Utils.getConfigForSerialization();
            while (true) {
              var len = in.readInt();
              byte buffer[] = new byte[len]; // this could be reused !
              while (len > 0)
                len -= in.read(buffer, buffer.length - len, len);
              // skipped: check for stream close
              Object obj = config.getObjectInput(buffer).readObject();
              if (obj instanceof Integer brkintval && brkintval == -1)
                break;
              bytesl.add((byte[]) obj);
            }
            String[] arguments = null;
            if (bytesl.size() > 2) {
              var totalargs = bytesl.size();
              arguments = new String[totalargs - 1];
              for (var index = 2; index < totalargs; index++) {
                arguments[index - 2] = new String(bytesl.get(index));
              }
            }
            // Execute concurrently through thread pool
            // executors.
            es.execute(new StreamPipelineTaskScheduler(cf, new String(bytesl.get(1)), bytesl.get(0),
                arguments, s));
          } catch (Exception ex) {
            log.error("Launching Stream Task scheduler error, See cause below \n", ex);
          }
        }
      } catch (Exception ex) {

      }
    });
    Utils.addShutdownHook(() -> {
      try {
        log.debug(STOPPINGANDCLOSECONNECTION);
        log.debug("Destroying...");
        if (!Objects.isNull(hbss)) {
          try {
            hbss.close();
          } catch (IOException e) {
            log.error(MDCConstants.EMPTY, e);
          }
        }
        if (!Objects.isNull(es)) {
          es.shutdown();
        }
        if (!Objects.isNull(esstream)) {
          esstream.shutdown();
        }
        if (!Objects.isNull(su)) {
          su.stop();
          su.destroy();
        }
        cdl.countDown();
        log.info("Faltering the stream...");
      } catch (Exception e) {
        log.error(MDCConstants.EMPTY, e);
      }
    });
  }

  @SuppressWarnings({"unchecked", "resource"})
  public static void startTaskScheduler(CuratorFramework cf, CountDownLatch cdl) throws Exception {
    var hbs = new HeartBeat();
    hbs.init(
        Integer
            .parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_RESCHEDULEDELAY)),
        Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT)),
        NetworkUtil
            .getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HOST)),
        Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_INITIALDELAY)),
        Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PINGDELAY)),
        "");
    hbs.start();
    var su = new ServerUtils();
    su.init(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_WEB_PORT)),
        new TaskSchedulerWebServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX,
        new WebResourcesServlet(), MDCConstants.FORWARD_SLASH + MDCConstants.RESOURCES
            + MDCConstants.FORWARD_SLASH + MDCConstants.ASTERIX);
    su.start();
    var es = Executors.newWorkStealingPool();
    var essingle = Executors.newSingleThreadExecutor();

    ZookeeperOperations.addconnectionstate.addConnectionStateListener(cf,
        (CuratorFramework cfclient, ConnectionState cs) -> {
          if (cs == ConnectionState.RECONNECTED) {
            var nodedata = NetworkUtil
                .getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HOST))
                + MDCConstants.UNDERSCORE
                + MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT);
            var nodesdata = (List<String>) ZookeeperOperations.nodesdata.invoke(cf,
                MDCConstants.ZK_BASE_PATH + MDCConstants.FORWARD_SLASH + MDCConstants.TASKSCHEDULER,
                null, null);
            if (!nodesdata.contains(nodedata))
              ZookeeperOperations.ephemeralSequentialCreate.invoke(cfclient,
                  MDCConstants.ZK_BASE_PATH + MDCConstants.FORWARD_SLASH
                      + MDCConstants.TASKSCHEDULER,
                  MDCConstants.TS + MDCConstants.HYPHEN, nodedata);
          }
        });
    ZookeeperOperations.ephemeralSequentialCreate.invoke(cf,
        MDCConstants.ZK_BASE_PATH + MDCConstants.FORWARD_SLASH + MDCConstants.TASKSCHEDULER,
        MDCConstants.TS + MDCConstants.HYPHEN,
        NetworkUtil
            .getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HOST))
            + MDCConstants.UNDERSCORE
            + MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT));

    var ss = Utils.createSSLServerSocket(
        Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_PORT)));
    essingle.execute(() -> {
      while (true) {
        try {
          var s = ss.accept();
          var bytesl = new ArrayList<byte[]>();

          var in = new DataInputStream(s.getInputStream());
          var config = Utils.getConfigForSerialization();
          while (true) {
            var len = in.readInt();
            byte buffer[] = new byte[len]; // this could be reused !
            while (len > 0)
              len -= in.read(buffer, buffer.length - len, len);
            // skipped: check for stream close
            Object obj = config.getObjectInput(buffer).readObject();
            if (obj instanceof Integer brkintval && brkintval == -1)
              break;
            bytesl.add((byte[]) obj);
          }
          String[] arguments = null;
          if (bytesl.size() > 2) {
            var totalargs = bytesl.size();
            arguments = new String[totalargs - 1];
            for (var index = 2; index < totalargs; index++) {
              arguments[index - 2] = new String(bytesl.get(index));
            }
          }
          es.execute(new TaskScheduler(cf, bytesl.get(0), arguments, s, new String(bytesl.get(1))));

        } catch (Exception ex) {
          log.error(MDCConstants.EMPTY, ex);
        }
      }
    });
    Utils.addShutdownHook(() -> {
      try {
        log.debug(STOPPINGANDCLOSECONNECTION);
        hbs.stop();
        hbs.destroy();
        log.debug("Destroying...");
        es.shutdown();
        essingle.shutdown();
        su.stop();
        su.destroy();
        cdl.countDown();
        log.debug("Halting...");
      } catch (Exception e) {
        log.error(MDCConstants.EMPTY, e);
      }
    });
  }
}
