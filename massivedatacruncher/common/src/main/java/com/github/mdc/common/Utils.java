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
package com.github.mdc.common;

import static java.util.Objects.nonNull;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.jgrapht.Graph;
import org.jgrapht.io.ComponentNameProvider;
import org.jgrapht.io.DOTExporter;
import org.jgrapht.io.ExportException;
import org.jgrapht.io.GraphExporter;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.util.UUID;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author arun Utils for adding the shutdown hook and obtaining the shuffled
 *         task executors and 
 *         utilities and send and receive the objects via socket.
 */
public class Utils {
	private static org.slf4j.Logger log = LoggerFactory.getLogger(Utils.class);

	private Utils() {
	}

	static MemoryPoolMXBean mpBeanLocalToJVM;

	static {
		for (MemoryPoolMXBean mpBean : ManagementFactory.getMemoryPoolMXBeans()) {
			if (mpBean.getType() == MemoryType.HEAP) {
				mpBeanLocalToJVM = mpBean;
				break;
			}
		}
	}

	/**
	 * Shutdown hook
	 * 
	 * @param runnable
	 */
	public static void addShutdownHook(Runnable runnable) {
		log.debug("Entered Utils.addShutdownHook");
		Runtime.getRuntime().addShutdownHook(new Thread(runnable));
		log.debug("Exiting Utils.addShutdownHook");
	}

	static ThreadLocal<FSTConfiguration> conf = new ThreadLocal<>() { 
		    public FSTConfiguration initialValue() {
		    	FSTConfiguration conf = FSTConfiguration.createUnsafeBinaryConfiguration();
		    	conf.setShareReferences(true);
		    	conf.setForceSerializable(true);
		    	conf.setPreferSpeed(true);
		    	conf.registerClass(CSVRecord.class);
		    	conf.registerClass(CSVParser.class);
		    	conf.registerClass(LinkedHashSet.class);
		    	return conf;
		    }
		};
	

	public static FSTConfiguration getConfigForSerialization() {
		return conf.get();
	}

	public static void writeToOstream(OutputStream os, String message) throws Exception {
		if (nonNull(os)) {
			os.write(message.getBytes());
			os.write('\n');
			os.flush();
		}
	}
	
	/**
	 * Serialize the object
	 * @param object
	 * @return serialized byte array
	 */
	public static Object serialiazeObject(Object object) {
		return conf.get().asByteArray(object);
	}
	
	/**
	 * DeSerialize the object
	 * @param object
	 * @return deserialized object
	 */
	public static Object deSerialiazeObject(byte[] object) {
		return conf.get().asObject(object);
	}

	
	
	/**
	 * This method configures the log4j properties and obtains the properties from
	 * the config folder in the binary distribution.
	 * 
	 * @param propertyfile
	 * @throws Exception
	 */
	public static void loadLog4JSystemProperties(String propertiesfilepath, String propertyfile) throws Exception {
		log.debug("Entered Utils.loadLog4JSystemProperties");		
		if (Objects.isNull(propertyfile)) {
			throw new Exception("Property File Name cannot be null");
		}
		if (Objects.isNull(propertiesfilepath)) {
			throw new Exception("Properties File Path cannot be null");
		}
		try (var fis = new FileInputStream(propertiesfilepath + propertyfile);) {
			PropertyConfigurator.configure(propertiesfilepath + MDCConstants.LOG4J_PROPERTIES);
			var prop = new Properties();
			prop.load(fis);
			prop.putAll(System.getProperties());
			log.debug("Properties: " + prop.entrySet());
			MDCProperties.put(prop);
		} catch (Exception ex) {
			log.error("Problem in loading properties, See the cause below", ex);
			throw new Exception("Unable To Load Properties", ex);
		}
		log.debug("Exiting Utils.loadLog4JSystemProperties");
	}

	/**
	 * This method configures the log4j properties and obtains the properties from
	 * the classpath in the binary distribution. This method is for testing
	 * purposes.
	 * 
	 * @param propertyfile
	 * @throws Exception
	 */
	public static void loadLog4JSystemPropertiesClassPath(String propertyfile) throws Exception {
		log.debug("Entered Utils.loadLog4JSystemPropertiesClassPath");
		if (Objects.isNull(propertyfile)) {
			throw new Exception("Property File Name cannot be null");
		}
		PropertyConfigurator.configure(
				Utils.class.getResourceAsStream(MDCConstants.FORWARD_SLASH + MDCConstants.LOG4J_PROPERTIES));
		var prop = new Properties();
		try {
			var fis = Utils.class.getResourceAsStream(MDCConstants.FORWARD_SLASH + propertyfile);
			prop.load(fis);
			prop.putAll(System.getProperties());
			log.debug("Property Names: " + prop.stringPropertyNames());
			MDCProperties.put(prop);
		} catch (Exception ex) {
			log.error("Problem in loading properties, See the cause below", ex);
			throw new Exception("Unable To Load Properties", ex);
		}
		log.debug("Exiting Utils.loadLog4JSystemPropertiesClassPath");
	}

	/**
	 * This method configures the log4j properties and obtains the properties from
	 * the classpath in the binary distribution for mesos.
	 * 
	 * @param propertyfile
	 * @throws Exception 
	 */
	public static void loadPropertiesMesos(String propertyfile) throws Exception {
		log.debug("Entered Utils.loadPropertiesMesos");
		PropertyConfigurator.configure(
				Utils.class.getResourceAsStream(MDCConstants.FORWARD_SLASH + MDCConstants.LOG4J_PROPERTIES));
		var prop = new Properties();
		try (var fis = Utils.class.getResourceAsStream(MDCConstants.FORWARD_SLASH + propertyfile);) {
			prop.load(fis);
			prop.putAll(System.getProperties());
			log.debug("Properties: " + prop.entrySet());
			MDCProperties.put(prop);
		} catch (Exception ex) {
			log.error("Problem in loading properties, See the cause below", ex);
		}
		log.debug("Exiting Utils.loadPropertiesMesos");
	}

	/**
	 * This function returns byte array to Character Stream
	 * 
	 * @param blockdata
	 * @return
	 */
	public static Stream<String> byteArrayToStringStream(byte[] blockdata) {
		log.debug("Entered Utils.byteArrayToStringStream");
		var is = new ByteArrayInputStream(blockdata);
		var bfReader = new BufferedReader(new InputStreamReader(is));
		log.debug("Exiting Utils.byteArrayToStringStream");
		return bfReader.lines().parallel();
	}

	/**
	 * This function creates and configures the jgroups channel object for the given
	 * input and returns it. This is used in jgroups mode of autonomous task
	 * execution with no scheduler behind it.
	 * 
	 * @param jobid
	 * @param networkaddress
	 * @param port
	 * @param mapreq
	 * @param mapresp
	 * @return jgroups channel object.
	 * @throws Exception
	 */
	public static JChannel getChannelTaskExecutor(String jobid, String networkaddress, int port,
			Map<String, WhoIsResponse.STATUS> mapreq, Map<String, WhoIsResponse.STATUS> mapresp) throws Exception {
		log.debug("Entered Utils.getChannelTaskExecutor");
		var channel = Utils.getChannelWithPStack(networkaddress);
		if (!Objects.isNull(channel)) {
			channel.setName(networkaddress + "_" + port);
			channel.setReceiver(new Receiver() {
				String jobidl = jobid;
				Map<String, WhoIsResponse.STATUS> mapreql = mapreq;
				Map<String, WhoIsResponse.STATUS> maprespl = mapresp;

				public void viewAccepted(View clusterview) {
				}

				public void receive(Message msg) {
					var rawbuffer = (byte[]) ((ObjectMessage) msg).getObject();
					try {
						var object = conf.get().asObject(rawbuffer);
						if (object instanceof WhoIsRequest whoisrequest) {
							if (mapreql.containsKey(whoisrequest.getStagepartitionid())) {
								log.debug("Whois: " + whoisrequest.getStagepartitionid() + " Map Status: " + mapreql
										+ " Map Response Status: " + maprespl);
								whoisresp(msg, whoisrequest.getStagepartitionid(), jobidl,
										mapreql.get(whoisrequest.getStagepartitionid()), channel, networkaddress);
							}
						} else if (object instanceof WhoIsResponse whoisresponse) {
							log.debug("WhoisResp: " + whoisresponse.getStagepartitionid() + " Status: "
									+ whoisresponse.getStatus());
							maprespl.put(whoisresponse.getStagepartitionid(), whoisresponse.getStatus());
						} else if (object instanceof WhoAreRequest) {
							log.debug("WhoAreReq: ");
							whoareresponse(channel, msg.getSrc(), mapreql);
						} else if (object instanceof WhoAreResponse whoareresponse) {
							log.debug("WhoAreResp: ");
							maprespl.putAll(whoareresponse.getResponsemap());
						}
					} catch (Exception ex) {

					}
				}
			});
			channel.setDiscardOwnMessages(true);
			channel.connect(jobid);
		}
		log.debug("Exiting Utils.getChannelTaskExecutor");
		return channel;
	}

	/**
	 * Request the status of the stage whoever is the executing the stage tasks.
	 * This method is used by the task executors.
	 * 
	 * @param channel
	 * @param stagepartitionid
	 * @throws Exception
	 */
	public static void whois(JChannel channel, String stagepartitionid) throws Exception {
		log.debug("Entered Utils.whois");
		var whoisrequest = new WhoIsRequest();
		whoisrequest.setStagepartitionid(stagepartitionid);
		try {
			channel.send(new ObjectMessage(null, conf.get().asByteArray(whoisrequest)));
		} finally {}
		log.debug("Exiting Utils.whois");
	}

	/**
	 * Request the status of the all the stages whoever are the executing the stage
	 * tasks. This method is used by the job scheduler in jgroups mode of stage task
	 * executions.
	 * 
	 * @param channel
	 * @throws Exception
	 */
	public static void whoare(JChannel channel) throws Exception {
		log.debug("Entered Utils.whoare");
		var whoarerequest = new WhoAreRequest();
		try {
			channel.send(new ObjectMessage(null, conf.get().asByteArray(whoarerequest)));
		} finally {}
		log.debug("Exiting Utils.whoare");
	}

	/**
	 * Response of the whoare request used by the schedulers.
	 * 
	 * @param channel
	 * @param address
	 * @param maptosend
	 * @throws Exception
	 */
	public static void whoareresponse(JChannel channel, Address address, Map<String, WhoIsResponse.STATUS> maptosend)
			throws Exception {
		log.debug("Entered Utils.whoareresponse");
		try {
			var whoareresp = new WhoAreResponse();
			whoareresp.setResponsemap(maptosend);
			channel.send(new ObjectMessage(address, conf.get().asByteArray(whoareresp)));
		} finally {}
		log.debug("Exiting Utils.whoareresponse");
	}

	/**
	 * Response of the whois request used by the task executors to execute the next
	 * stage tasks.
	 * 
	 * @param msg
	 * @param stagepartitionid
	 * @param jobid
	 * @param status
	 * @param jchannel
	 * @param networkaddress
	 * @throws Exception
	 */
	public static void whoisresp(Message msg, String stagepartitionid, String jobid, WhoIsResponse.STATUS status,
			JChannel jchannel, String networkaddress) throws Exception {
		log.debug("Entered Utils.whoisresp");
		var whoisresponse = new WhoIsResponse();
		whoisresponse.setStagepartitionid(stagepartitionid);
		whoisresponse.setStatus(status);
		try {
			jchannel.send(new ObjectMessage(msg.getSrc(), conf.get().asByteArray(whoisresponse)));
		} finally {}
		log.debug("Exiting Utils.whoisresp");
	}

	/**
	 * This method stores graph information of stages in file.
	 * 
	 * @param graph
	 * @param writer
	 * @throws ExportException
	 */
	public static void renderGraphStage(Graph<Stage, DAGEdge> graph, Writer writer) throws ExportException {
		log.debug("Entered Utils.renderGraphStage");
		ComponentNameProvider<Stage> vertexIdProvider = stage -> {

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				log.warn("Interrupted!", e);
				// Restore interrupted state...
				Thread.currentThread().interrupt();
			} catch (Exception ex) {
				log.error("Delay Error, see cause below \n", ex);
			}
			return "" + System.currentTimeMillis();

		};
		ComponentNameProvider<Stage> vertexLabelProvider = Stage::toString;
		GraphExporter<Stage, DAGEdge> exporter = new DOTExporter<>(vertexIdProvider, vertexLabelProvider, null);
		exporter.exportGraph(graph, writer);
		var path = MDCProperties.get().getProperty(MDCConstants.GRAPDIRPATH);
		new File(path).mkdirs();
		try (var stagegraphfile = new FileWriter(
				path + MDCProperties.get().getProperty(MDCConstants.GRAPHFILESTAGESPLANNAME)
						+ System.currentTimeMillis());) {
			stagegraphfile.write(writer.toString());
		} catch (Exception e) {
			log.error("File Write Error, see cause below \n", e);
		}
		log.debug("Exiting Utils.renderGraphStage");
	}

	/**
	 * This method stores graph information of physical execution plan in file.
	 * 
	 * @param graph
	 * @param writer
	 * @throws ExportException
	 */
	public static void renderGraphPhysicalExecPlan(Graph<Task, DAGEdge> graph, Writer writer) throws ExportException {
		log.debug("Entered Utils.renderGraphPhysicalExecPlan");
		ComponentNameProvider<Task> vertexIdProvider = jobstage -> {

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				log.warn("Interrupted!", e);
				// Restore interrupted state...
				Thread.currentThread().interrupt();
			} catch (Exception ex) {
				log.error("Delay Error, see cause below \n", ex);
			}
			return "" + System.currentTimeMillis();

		};
		ComponentNameProvider<Task> vertexLabelProvider = Task::toString;
		var exporter = new DOTExporter<Task, DAGEdge>(vertexIdProvider, vertexLabelProvider, null);
		exporter.exportGraph(graph, writer);
		var path = MDCProperties.get().getProperty(MDCConstants.GRAPDIRPATH);
		new File(path).mkdirs();
		try (var stagegraphfile = new FileWriter(path
				+ MDCProperties.get().getProperty(MDCConstants.GRAPHFILEPEPLANNAME) + System.currentTimeMillis());) {
			stagegraphfile.write(writer.toString());
		} catch (Exception e) {
			log.error("File Write Error, see cause below \n", e);
		}
		log.debug("Exiting Utils.renderGraphPhysicalExecPlan");
	}

	/**
	 * This function returns the GC status.
	 * 
	 * @return garbage collectors status.
	 */
	public static String getGCStats() {
		log.debug("Entered Utils.getGCStats");
		var totalGarbageCollections = 0;
		var garbageCollectionTime = 0;
		for (var gc : ManagementFactory.getGarbageCollectorMXBeans()) {
			var count = gc.getCollectionCount();

			if (count >= 0) {
				totalGarbageCollections += count;
			}

			var time = gc.getCollectionTime();

			if (time >= 0) {
				garbageCollectionTime += time;
			}
		}
		log.debug("Exiting Utils.getGCStats");
		return "Garbage Collections: " + totalGarbageCollections + " n " + "Garbage Collection Time (ms): "
				+ garbageCollectionTime;
	}


	/**
	 * This function returns the object by socket using the host port of the server
	 * and the input object to the server.
	 * 
	 * @param hp
	 * @param inputobj
	 * @return object
	 * @throws Exception 
	 */
	public static Object getResultObjectByInput(String hp, Object inputobj) throws Exception {
		var hostport = hp.split(MDCConstants.UNDERSCORE);
		try {
			final Registry registry = LocateRegistry.getRegistry(hostport[0], Integer.parseInt(hostport[1]));
			StreamDataCruncher cruncher = (StreamDataCruncher) registry.lookup(MDCConstants.BINDTESTUB);
			return cruncher.postObject(inputobj);
		} catch (Exception ex) {
			log.error("Unable to read result Object: " + inputobj + " " + hp, ex);
			throw ex;
		}
	}

	/**
	 * 
	 * @param bindaddr
	 * @return jgroups channel object
	 */
	public static synchronized JChannel getChannelWithPStack(String bindaddr) {
		try {
			System.setProperty(MDCConstants.BINDADDRESS, bindaddr);
			String configfilepath = System.getProperty(MDCConstants.USERDIR) + MDCConstants.FORWARD_SLASH
					+ MDCProperties.get().getProperty(MDCConstants.JGROUPSCONF);
			log.info("Composing Jgroups for address latch {} with trail {}", bindaddr, configfilepath);
			var channel = new JChannel(configfilepath);
			return channel;
		} catch (Exception ex) {
			log.error("Unable to add Protocol Stack: ", ex);
		}
		return null;
	}

	public static synchronized JChannel getChannelTSSHA(String bindaddress, Receiver receiver) throws Exception {
		JChannel channel = getChannelWithPStack(
				bindaddress);
		if (!Objects.isNull(channel)) {
			channel.setName(bindaddress);
			channel.setDiscardOwnMessages(true);
			if (!Objects.isNull(receiver)) {
				channel.setReceiver(receiver);
			}
			channel.connect(MDCConstants.TSSHA);
		}
		return channel;
	}

	public static List<String> getAllFilePaths(List<Path> paths) {
		return paths.stream().map(path -> path.toUri().toString()).collect(Collectors.toList());
	}

	public static long getTotalLengthByFiles(FileSystem hdfs, List<Path> paths) throws IOException {
		long totallength = 0;
		for (var filepath : paths) {
			var fs = (DistributedFileSystem) hdfs;
			var dis = fs.getClient().open(filepath.toUri().getPath());
			totallength += dis.getFileLength();
			dis.close();
		}
		return totallength;
	}

	public static void createJar(File folder, String outputfolder, String outjarfilename) {
		var manifest = new Manifest();
		manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
		try (var target = new JarOutputStream(
				new FileOutputStream(outputfolder + MDCConstants.FORWARD_SLASH + outjarfilename), manifest);) {
			add(folder, target);
		} catch (IOException ioe) {
			log.error("Unable to create Jar", ioe);
		}
	}

	private static void add(File source, JarOutputStream target) throws IOException {
		BufferedInputStream in = null;
		try {
			if (source.isDirectory()) {
				for (var nestedFile : source.listFiles())
					add(nestedFile, target);
				return;
			}

			var entry = new JarEntry(source.getName());
			entry.setTime(source.lastModified());
			target.putNextEntry(entry);
			in = new BufferedInputStream(new FileInputStream(source));

			var buffer = new byte[1024];
			while (true) {
				var count = in.read(buffer);
				if (count == -1) {
					break;
				}
				target.write(buffer, 0, count);
			}
			target.closeEntry();
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}
	
	
	public static String getCacheID() {
		return UUID.randomUUID().toString();
	}
	
	private static final AtomicInteger uniqueidincrement = new AtomicInteger(1) ;
	public static int getUniqueID() {
		return uniqueidincrement.getAndIncrement();
	}

	private static final AtomicInteger uniquetaskidincrement = new AtomicInteger(1) ;
	public static int getUniqueTaskID() {
		return uniquetaskidincrement.getAndIncrement();
	}

	private static final AtomicInteger uniquestageidincrement = new AtomicInteger(1) ;
	public static int getUniqueStageID() {
		return uniquestageidincrement.getAndIncrement();
	}

	private static final AtomicInteger uniquejobidincrement = new AtomicInteger(1) ;
	public static int getUniqueJobID() {
		return uniquejobidincrement.getAndIncrement();
	}

	private static final AtomicInteger uniqueappidincrement = new AtomicInteger(1) ;
	public static int getUniqueAppID() {
		return uniqueappidincrement.getAndIncrement();
	}

	public static ServerCnxnFactory startZookeeperServer(int clientport, int numconnections, int ticktime)
			throws Exception {
		var dataDirectory = System.getProperty("java.io.tmpdir");
		var dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();
		var server = new ZooKeeperServer(dir, dir, ticktime);
		ServerCnxnFactory scf = ServerCnxnFactory.createFactory(new InetSocketAddress(clientport), numconnections);
		scf.startup(server);
		return scf;
	}

	public static boolean getZGCMemUsage(float percentage) {
		MemoryUsage musage = mpBeanLocalToJVM.getCollectionUsage();
		float memusage = (float) (musage.getUsed() / (float) musage.getMax() * 100.0);
		return memusage >= percentage;
	}

	public static void writeResultToHDFS(String hdfsurl, String filepath, InputStream is) throws Exception {
		try (var hdfs = FileSystem.get(new URI(hdfsurl), new Configuration());
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(hdfsurl + filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)))));
				FSTObjectInput in = new FSTObjectInput(is);) {
			while (is.available() > 0) {				
				Object result = in.readObject();
				if (result instanceof List res) {
					for (var value : res) {
						bw.write(value.toString());
						bw.write(MDCConstants.NEWLINE);
					}
				} else {
					bw.write(result.toString());
				}
			}
			bw.flush();
		} catch (IOException ioe) {
		} catch (Exception e) {
			log.error(PipelineConstants.FILEIOERROR, e);
			throw new Exception(PipelineConstants.FILEIOERROR, e);
		}

	}

	public static void writeResultToHDFS(String hdfsurl, String filepath, Object out) throws Exception {
		try (var hdfs = FileSystem.get(new URI(hdfsurl), new Configuration());
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(hdfsurl + filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)))));) {
			if (out instanceof List res) {
				for (var value : res) {
					bw.write(value.toString());
					bw.write(MDCConstants.NEWLINE);
				}
			} else {
				bw.write(out.toString());
			}
			bw.flush();
		} catch (IOException ioe) {
		} catch (Exception e) {
			log.error(PipelineConstants.FILEIOERROR, e);
			throw new Exception(PipelineConstants.FILEIOERROR, e);
		}
	}

	public static String getIntermediateInputStreamRDF(RemoteDataFetch rdf) throws Exception {
		log.debug("Entered Utils.getIntermediateInputStreamRDF");
		var path = rdf.getJobid() + MDCConstants.HYPHEN + rdf.getStageid() + MDCConstants.HYPHEN + rdf.getTaskid();
		log.debug("Returned Utils.getIntermediateInputStreamRDF");
		return path;
	}

	public static String getIntermediateInputStreamTask(Task task) throws Exception {
		log.debug("Entered Utils.getIntermediateInputStreamTask");
		var path = task.jobid + MDCConstants.HYPHEN + task.stageid + MDCConstants.HYPHEN + task.taskid;
		log.debug("Returned Utils.getIntermediateInputStreamTask");
		return path;
	}

	@SuppressWarnings("unchecked")
	public static String launchContainers(Integer numberofcontainers) throws Exception {
		var containerid = MDCConstants.CONTAINER + MDCConstants.HYPHEN + Utils.getUniqueID();
		var jobid = MDCConstants.JOB + MDCConstants.HYPHEN + Utils.getUniqueJobID();
		var ac = new AllocateContainers();
		ac.setContainerid(containerid);
		ac.setNumberofcontainers(numberofcontainers);
		var nrs = MDCNodesResources.get();
		var resources = nrs.values();
		int numavailable = Math.min(numberofcontainers, resources.size());
		Iterator<Resources> res = resources.iterator();
		var lcs = new ArrayList<LaunchContainers>();
		for (int container = 0; container < numavailable; container++) {
			Resources restolaunch = res.next();
			List<Integer> ports = (List<Integer>) Utils.getResultObjectByInput(restolaunch.getNodeport(), ac);
			if (Objects.isNull(ports)) {
				throw new ContainerException("Port Allocation Error From Container");
			}
			log.info("Chamber alloted with node: " + restolaunch.getNodeport() + " amidst ports: " + ports);
			var cla = new ContainerLaunchAttributes();
			var crs = new ContainerResources();
			crs.setPort(ports.get(0));
			crs.setCpu(restolaunch.getNumberofprocessors());
			var meminmb = restolaunch.getFreememory() / MDCConstants.MB;
			var heapmem = meminmb * 30 / 100;
			crs.setMinmemory(heapmem);
			crs.setMaxmemory(heapmem);
			crs.setDirectheap(meminmb - heapmem);
			crs.setGctype(MDCConstants.ZGC);
			cla.setCr(Arrays.asList(crs));
			cla.setNumberofcontainers(1);
			LaunchContainers lc = new LaunchContainers();
			lc.setCla(cla);
			lc.setNodehostport(restolaunch.getNodeport());
			lc.setContainerid(containerid);
			lc.setJobid(jobid);
			List<Integer> launchedcontainerports = (List<Integer>) Utils.getResultObjectByInput(lc.getNodehostport(),
					lc);
			if (Objects.isNull(launchedcontainerports)) {
				throw new ContainerException("Task Executor Launch Error From Container");
			}
			int index = 0;
			while (index < launchedcontainerports.size()) {
				while (true) {
					String tehost = lc.getNodehostport().split("_")[0];
					try (var sock = Utils.createSSLSocket(tehost, launchedcontainerports.get(index));) {
						break;
					} catch (Exception ex) {
						try {
							log.info("Waiting for chamber " + tehost + MDCConstants.UNDERSCORE
									+ launchedcontainerports.get(index) + " to replete dispatch....");
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							log.warn("Interrupted!", e);
							// Restore interrupted state...
							Thread.currentThread().interrupt();
						} catch (Exception e) {
							log.error(MDCConstants.EMPTY, e);
						}
					}
				}
				index++;
			}
			log.info("Chamber dispatched node: " + restolaunch.getNodeport() + " with ports: "
					+ launchedcontainerports);
			lcs.add(lc);
		}
		GlobalContainerLaunchers.put(containerid, lcs);
		return containerid;
	}

	public static void destroyContainers(String containerid) {
		var dc = new DestroyContainers();
		dc.setContainerid(containerid);
		var lcs = GlobalContainerLaunchers.get(containerid);
		lcs.stream().forEach(lc -> {
			try {
				Utils.getResultObjectByInput(lc.getNodehostport(), lc);
			} catch (Exception e) {
				log.error(MDCConstants.EMPTY, e);
			}
		});
		GlobalContainerLaunchers.remove(containerid);
	}
	static List<Object> objects = new ArrayList<>();
	public static Registry getRPCRegistry(int port, final StreamDataCruncher streamdatacruncher) throws Exception {
		objects.add(streamdatacruncher);
		Registry registry = LocateRegistry.createRegistry(port);
		StreamDataCruncher stub = (StreamDataCruncher) UnicastRemoteObject.exportObject(streamdatacruncher, 0);
		registry.rebind(MDCConstants.BINDTESTUB, stub);
		return registry;
	}


	public static ServerSocket createSSLServerSocket(int port) throws Exception {
		KeyStore ks = KeyStore.getInstance("JKS");
		String password = MDCProperties.get().getProperty(MDCConstants.MDC_KEYSTORE_PASSWORD);
		ks.load(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.MDC_JKS)), password.toCharArray());

		KeyManagerFactory kmf = KeyManagerFactory.getInstance(MDCProperties.get().getProperty(MDCConstants.MDC_JKS_ALGO));
		kmf.init(ks, password.toCharArray());

		TrustManagerFactory tmf = TrustManagerFactory.getInstance(MDCProperties.get().getProperty(MDCConstants.MDC_JKS_ALGO));
		tmf.init(ks);

		SSLContext sc = SSLContext.getInstance("TLS");
		TrustManager[] trustManagers = tmf.getTrustManagers();
		sc.init(kmf.getKeyManagers(), trustManagers, null);
		SSLServerSocketFactory ssf = sc.getServerSocketFactory();
		SSLServerSocket sslserversocket = (SSLServerSocket) ssf.createServerSocket();
		sslserversocket.bind(new InetSocketAddress(InetAddress.getByAddress(new byte[]{0x00, 0x00, 0x00, 0x00}), port), 256);
		return sslserversocket;
	}
	public static Socket createSSLSocket(String host, int port) throws Exception {
		log.info("Constructing socket factory for the (host,port): ("+host+"," +port+")");
		KeyStore ks = KeyStore.getInstance("JKS");
		String password = MDCProperties.get().getProperty(MDCConstants.MDC_KEYSTORE_PASSWORD);
		ks.load(new FileInputStream(MDCProperties.get().getProperty(MDCConstants.MDC_JKS)), password.toCharArray());

		KeyManagerFactory kmf = KeyManagerFactory.getInstance(MDCProperties.get().getProperty(MDCConstants.MDC_JKS_ALGO));
		kmf.init(ks, password.toCharArray());

		TrustManagerFactory tmf = TrustManagerFactory.getInstance(MDCProperties.get().getProperty(MDCConstants.MDC_JKS_ALGO));
		tmf.init(ks);

		SSLContext sc = SSLContext.getInstance("TLS");
		TrustManager[] trustManagers = tmf.getTrustManagers();
		sc.init(kmf.getKeyManagers(), trustManagers, null);

		SSLSocketFactory sf = sc.getSocketFactory();
		log.info("Constructing SSLSocket for the (host,port): ("+host+"," +port+")");
		SSLSocket sslsocket = (SSLSocket) sf.createSocket(host, port);
		log.info("Kickoff SSLHandshake for the (host,port): ("+host+"," +port+")");
		sslsocket.startHandshake();
		log.info("SSLHandshake concluded for the (host,port): ("+host+"," +port+")");
		return sslsocket;
	}
}
