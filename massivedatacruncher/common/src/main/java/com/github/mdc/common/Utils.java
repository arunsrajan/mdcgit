package com.github.mdc.common;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import java.lang.invoke.SerializedLambda;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.lang.reflect.InvocationHandler;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.jgrapht.Graph;
import org.jgrapht.graph.SimpleDirectedGraph;
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
import org.jooq.lambda.tuple.Tuple2;
import org.json.simple.JSONObject;
import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer.CompatibleFieldSerializerConfig;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;

import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyMapSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptySetSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonMapSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonSetSerializer;
import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
import de.javakaffee.kryoserializers.JdkProxySerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.cglib.CGLibProxySerializer;
import de.javakaffee.kryoserializers.guava.ArrayListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ArrayTableSerializer;
import de.javakaffee.kryoserializers.guava.HashBasedTableSerializer;
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSetSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableTableSerializer;
import de.javakaffee.kryoserializers.guava.LinkedHashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.LinkedListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ReverseListSerializer;
import de.javakaffee.kryoserializers.guava.TreeBasedTableSerializer;
import de.javakaffee.kryoserializers.guava.TreeMultimapSerializer;
import de.javakaffee.kryoserializers.guava.UnmodifiableNavigableSetSerializer;

/**
 * 
 * @author arun
 * Utils for adding the shutdown hook and obtaining the shuffled
 * task executors and much more on kryos and jgroups mode task execution
 * utilities and send and receive the objects via socket.
 */
public class Utils {
	private static Logger log = Logger.getLogger(Utils.class);

	private Utils() {
	}

	static MemoryPoolMXBean mpBeanLocalToJVM = null;
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

	private static Semaphore kryowritesem = new Semaphore(1);
	
	/**
	 * This method writes object to outputstream via kryo object without class information.
	 * @param kryo
	 * @param output
	 * @param outvalue
	 */
	public static void writeKryoOutput(Kryo kryo, Output output, Object outvalue) {
		try {
			kryowritesem.acquire();
			log.debug("Entered Utils.writeKryoOutput");
			if (!Objects.isNull(output)) {
				kryo.writeObject(output, MDCConstants.NEWLINE + outvalue);
				output.flush();
			}
			log.debug("Exiting Utils.writeKryoOutput");
		} catch (Exception e) {
			log.error(MDCConstants.EMPTY, e);
		} finally {
			kryowritesem.release();
		}
	}

	/**
	 * This method writes object to outputstream via kryo object with class information.
	 * @param kryo
	 * @param output
	 * @param outvalue
	 */
	public static void writeKryoOutputClassObject(Kryo kryo, Output output, Object outvalue) {
		log.debug("Entered Utils.writeKryoOutputClassObject");
		kryo.writeClassAndObject(output, outvalue);
		output.flush();
		log.debug("Exiting Utils.writeKryoOutputClassObject");
	}

	/**
	 * This method reads object from inputputstream via kryo object with class information.
	 * @param kryo
	 * @param input
	 * @return
	 */
	public static Object readKryoInputObjectWithClass(Kryo kryo, Input input) {
		return kryo.readClassAndObject(input);
	}

	/**
	 * This function returns kryo object with the registered classes with serializers.
	 * @return kryo object with the registered classes with serializers.
	 */
	public static Kryo getKryoNonDeflateSerializer() {
		log.debug("Entered Utils.getKryoNonDeflateSerializer");
		var kryo = new Kryo();
		registerKryoNonDeflateSerializer(kryo);
		log.debug("Exiting Utils.getKryoNonDeflateSerializer");
		return kryo;
	}

	/**
	 * This function returns kryo object with the registered classes with serializers.
	 * @return kryo object
	 */
	public static Kryo getKryo() {
		var kryo = new Kryo();
		getKryo(kryo);
		return kryo;
	}

	/**
	 * This method configures kryo object with the registered classes with serializers.
	 * @param kryo
	 */
	public static void getKryo(Kryo kryo) {
		RegisterKyroSerializers.register(kryo);
		var is = new StdInstantiatorStrategy();
		var dis = new DefaultInstantiatorStrategy(is);
		kryo.setInstantiatorStrategy(dis);
		dis.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
		kryo.register(Vector.class);
		kryo.register(ArrayList.class);
		kryo.register(SerializedLambda.class);
		kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());
		kryo.setRegistrationRequired(false);
		kryo.setReferences(true);
	}

	/**
	 * This method configures kryo object with the registered classes with serializers.
	 * @param kryo
	 */
	public static void registerKryoNonDeflateSerializer(Kryo kryo) {
		log.debug("Entered Utils.registerKryoNonDeflateSerializer");
		kryo.setRegistrationRequired(false);
		kryo.setReferences(true);
		kryo.setWarnUnregisteredClasses(false);
		RegisterKyroSerializers.register(kryo);
		var dis = new com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy(new StdInstantiatorStrategy());
		kryo.setInstantiatorStrategy(dis);
		dis.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
		kryo.setDefaultSerializer(FieldSerializer.class);
		kryo.register(Object[].class);
		kryo.register(Object.class);
		kryo.register(Class.class);
		kryo.register(MDCConstants.STORAGE.class);
		kryo.register(SerializedLambda.class);
		kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());
		kryo.register(Vector.class);
		kryo.register(ArrayList.class);
		CompatibleFieldSerializerConfig configtuple2 = new CompatibleFieldSerializerConfig();
		configtuple2.setFieldsCanBeNull(true);
		CompatibleFieldSerializer<Tuple2> cfs = new CompatibleFieldSerializer<Tuple2>(kryo,Tuple2.class,configtuple2);
		kryo.register(Tuple2.class, cfs);
		kryo.register(LinkedHashSet.class);
		kryo.register(Tuple2Serializable.class);
		CompatibleFieldSerializerConfig config = new CompatibleFieldSerializerConfig();
		config.setSerializeTransient(true);
		CompatibleFieldSerializer<CSVRecord> withTransient = new CompatibleFieldSerializer<CSVRecord>(kryo,
				CSVRecord.class, config);
		kryo.register(BlocksLocation.class);		
		kryo.register(RetrieveData.class);
		kryo.register(RetrieveKeys.class);
		kryo.register(Block.class);
		kryo.register(Block[].class);
		kryo.register(ConcurrentHashMap.class);
		kryo.register(byte[].class);
		kryo.register(LoadJar.class);
		kryo.register(JobStage.class);
		kryo.register(RemoteDataFetch[].class);
		kryo.register(RemoteDataFetch.class);
		kryo.register(Stage.class);
		kryo.register(Task.class);
		kryo.register(TasksGraphExecutor.class);
		kryo.register(DAGEdge.class);
		kryo.register(DataCruncherContext.class);
		kryo.register(JSONObject.class);
		kryo.register(PipelineConfig.class);
		kryo.register(SimpleDirectedGraph.class);
		kryo.register(CSVParser.class);
		kryo.register(CSVRecord.class, withTransient);
		kryo.register(ReducerValues.class);
		kryo.register(Task.class);
		kryo.register(SkipToNewLine.class);
		kryo.register(CloseStagesGraphExecutor.class);
		kryo.register(CloseStagesGraphExecutor.class, new CompatibleFieldSerializer(kryo,CloseStagesGraphExecutor.class));
		log.debug("Exiting Utils.registerKryoNonDeflateSerializer");
	}	
	
	/**
	 * This method configures the log4j properties and 
	 * obtains the properties from the config folder in the
	 * binary distribution.
	 * @param propertyfile
	 * @throws Exception 
	 */
	public static void loadLog4JSystemProperties(String propertiesfilepath, String propertyfile) throws Exception {
		log.debug("Entered Utils.loadLog4JSystemProperties");
		if(Objects.isNull(propertyfile)) {
			throw new Exception("Property File Name cannot be null");
		}
		if(Objects.isNull(propertiesfilepath)) {
			throw new Exception("Properties File Path cannot be null");
		}
		try (var fis = new FileInputStream(propertiesfilepath+propertyfile);) {
			PropertyConfigurator.configure(propertiesfilepath + MDCConstants.LOG4J_PROPERTIES);
			var prop = new Properties();
			prop.load(fis);
			prop.putAll(System.getProperties());
			log.debug("Properties: " + prop.entrySet());
			MDCProperties.put(prop);
		} catch (Exception ex) {
			log.error("Problem in loading properties, See the cause below", ex);
			throw new Exception("Unable To Load Properties",ex);
		}
		log.debug("Exiting Utils.loadLog4JSystemProperties");
	}
	/**
	 * This method configures the log4j properties and 
	 * obtains the properties from the classpath in the
	 * binary distribution. This method is for testing purposes.
	 * @param propertyfile
	 * @throws Exception 
	 */
	public static void loadLog4JSystemPropertiesClassPath(String propertyfile) throws Exception {
		log.debug("Entered Utils.loadLog4JSystemPropertiesClassPath");
		if(Objects.isNull(propertyfile)) {
			throw new Exception("Property File Name cannot be null");
		}
		PropertyConfigurator.configure(
				Utils.class.getResourceAsStream(MDCConstants.BACKWARD_SLASH + MDCConstants.LOG4J_PROPERTIES));
		var prop = new Properties();
		try {
			var fis = Utils.class.getResourceAsStream(MDCConstants.BACKWARD_SLASH + propertyfile);
			prop.load(fis);
			prop.putAll(System.getProperties());
			log.debug("Property Names: " + prop.stringPropertyNames());
			MDCProperties.put(prop);
		} catch (Exception ex) {
			log.error("Problem in loading properties, See the cause below", ex);
			throw new Exception("Unable To Load Properties",ex);
		}
		log.debug("Exiting Utils.loadLog4JSystemPropertiesClassPath");
	}
	/**
	 * This method configures the log4j properties and 
	 * obtains the properties from the classpath in the
	 * binary distribution for mesos.
	 * @param propertyfile
	 */
	public static void loadPropertiesMesos(String propertyfile) {
		log.debug("Entered Utils.loadPropertiesMesos");
		PropertyConfigurator.configure(
				Utils.class.getResourceAsStream(MDCConstants.BACKWARD_SLASH + MDCConstants.LOG4J_PROPERTIES));
		var prop = new Properties();
		try (var fis = Utils.class.getResourceAsStream(MDCConstants.BACKWARD_SLASH + propertyfile);) {
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
	 * This function creates and configures the jgroups channel object 
	 * for the given input and returns it. This is used in jgroups 
	 * mode of autonomous task execution with no scheduler behind it.
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
		channel.setName(networkaddress + "_" + port);
		channel.setReceiver(new Receiver() {
			String jobidl = jobid;
			Map<String, WhoIsResponse.STATUS> mapreql = mapreq;
			Map<String, WhoIsResponse.STATUS> maprespl = mapresp;

			public void viewAccepted(View clusterview) {
			}

			public void receive(Message msg) {
				var rawbuffer = (byte[])((ObjectMessage)msg).getObject();
				var kryo = getKryo();
				try (var bais = new ByteArrayInputStream(rawbuffer); var input = new Input(bais);) {
					var object = readKryoInputObjectWithClass(kryo, input);
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
		log.debug("Exiting Utils.getChannelTaskExecutor");
		return channel;
	}

	/**
	 * Request the status of the stage whoever is the executing the stage tasks.
	 * This method is used by the task executors.
	 * @param channel
	 * @param stagepartitionid
	 * @throws Exception
	 */
	public static void whois(JChannel channel, String stagepartitionid) throws Exception {
		log.debug("Entered Utils.whois");
		var whoisrequest = new WhoIsRequest();
		whoisrequest.setStagepartitionid(stagepartitionid);
		var kryo = getKryo();
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			writeKryoOutputClassObject(kryo, output, whoisrequest);
			channel.send(new ObjectMessage(null, baos.toByteArray()));
		} finally {

		}
		log.debug("Exiting Utils.whois");
	}
	
	/**
	 * Request the status of the all the stages whoever are the executing the stage tasks.
	 * This method is used by the job scheduler in jgroups mode of stage task executions.
	 * @param channel
	 * @param stagepartitionid
	 * @throws Exception
	 */
	public static void whoare(JChannel channel) throws Exception {
		log.debug("Entered Utils.whoare");
		var whoarerequest = new WhoAreRequest();
		var kryo = getKryo();
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			writeKryoOutputClassObject(kryo, output, whoarerequest);
			channel.send(new ObjectMessage(null, baos.toByteArray()));
		} finally {

		}
		log.debug("Exiting Utils.whoare");
	}

	/**
	 * Response of the whoare request used by the schedulers.
	 * @param channel
	 * @param address
	 * @param maptosend
	 * @throws Exception
	 */
	public static void whoareresponse(JChannel channel, Address address, Map<String, WhoIsResponse.STATUS> maptosend)
			throws Exception {
		log.debug("Entered Utils.whoareresponse");
		var kryo = getKryo();
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			var whoareresp = new WhoAreResponse();
			whoareresp.setResponsemap(maptosend);
			writeKryoOutputClassObject(kryo, output, whoareresp);
			channel.send(new ObjectMessage(address, baos.toByteArray()));
		} finally {

		}
		log.debug("Exiting Utils.whoareresponse");
	}

	/**
	 * Response of the whois request used by the task executors to 
	 * execute the next stage tasks.
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
		var kryo = getKryo();
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			writeKryoOutputClassObject(kryo, output, whoisresponse);
			jchannel.send(new ObjectMessage(msg.getSrc(), baos.toByteArray()));
		} finally {

		}
		log.debug("Exiting Utils.whoisresp");
	}

	/**
	 * This method stores graph information of stages in file.
	 * @param graph
	 * @param writer
	 * @throws ExportException
	 */
	public static void renderGraphStage(Graph<Stage, DAGEdge> graph, Writer writer) throws ExportException {
		log.debug("Entered Utils.renderGraphStage");
		ComponentNameProvider<Stage> vertexIdProvider = stage -> {

			try {
				Thread.sleep(500);
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
	 * @param graph
	 * @param writer
	 * @throws ExportException
	 */
	public static void renderGraphPhysicalExecPlan(Graph<Task, DAGEdge> graph, Writer writer)
			throws ExportException {
		log.debug("Entered Utils.renderGraphPhysicalExecPlan");
		ComponentNameProvider<Task> vertexIdProvider = jobstage -> {

			try {
				Thread.sleep(500);
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
	 * This function returns the kryo object configured
	 * for mesos mode execution.
	 * @return kryo object
	 */
	public static Kryo getKryoMesos() {
		log.debug("Entered Utils.getKryoMesos");
		var kryo = new Kryo();
		var is = new StdInstantiatorStrategy();
		var dis = new DefaultInstantiatorStrategy(is);
		kryo.setInstantiatorStrategy(dis);
		kryo.register(Object[].class);
		kryo.register(Object.class);
		kryo.register(Class.class);
		kryo.register(SerializedLambda.class);
		kryo.register(Vector.class);
		kryo.register(ArrayList.class);
		kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
		kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());
		log.debug("Exiting Utils.getKryoMesos");
		return kryo;
	}

	/**
	 * Configures the kryo object with custom serializers for the collection and 
	 * guava objects for serialization and deserialization. 
	 * @param kryo
	 */
	public static void registerKryoResult(Kryo kryo) {
		log.debug("Entered Utils.registerKryoResult");
		var dis = new DefaultInstantiatorStrategy(new StdInstantiatorStrategy());
		kryo.setInstantiatorStrategy(dis);
		dis.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
		kryo.register(Arrays.asList().getClass(), new ArraysAsListSerializer());
		kryo.register(Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer());
		kryo.register(Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer());
		kryo.register(Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer());
		kryo.register(Collections.singletonList(MDCConstants.EMPTY).getClass(),
				new CollectionsSingletonListSerializer());
		kryo.register(Collections.singleton(MDCConstants.EMPTY).getClass(), new CollectionsSingletonSetSerializer());
		kryo.register(Collections.singletonMap(MDCConstants.EMPTY, MDCConstants.EMPTY).getClass(),
				new CollectionsSingletonMapSerializer());
		kryo.register(GregorianCalendar.class, new GregorianCalendarSerializer());
		kryo.register(InvocationHandler.class, new JdkProxySerializer());
		UnmodifiableCollectionsSerializer.registerSerializers(kryo);
		SynchronizedCollectionsSerializer.registerSerializers(kryo);
		kryo.register(CGLibProxySerializer.CGLibProxyMarker.class, new CGLibProxySerializer());
		ImmutableListSerializer.registerSerializers(kryo);
		ImmutableSetSerializer.registerSerializers(kryo);
		ImmutableMapSerializer.registerSerializers(kryo);
		ImmutableMultimapSerializer.registerSerializers(kryo);
		ImmutableTableSerializer.registerSerializers(kryo);
		ReverseListSerializer.registerSerializers(kryo);
		UnmodifiableNavigableSetSerializer.registerSerializers(kryo);
		ArrayListMultimapSerializer.registerSerializers(kryo);
		HashMultimapSerializer.registerSerializers(kryo);
		LinkedHashMultimapSerializer.registerSerializers(kryo);
		LinkedListMultimapSerializer.registerSerializers(kryo);
		TreeMultimapSerializer.registerSerializers(kryo);
		ArrayTableSerializer.registerSerializers(kryo);
		HashBasedTableSerializer.registerSerializers(kryo);
		TreeBasedTableSerializer.registerSerializers(kryo);
		kryo.register(Object.class);
		kryo.register(Object[].class);
		kryo.register(Class.class);
		kryo.register(SerializedLambda.class);
		kryo.register(Vector.class);
		kryo.register(ArrayList.class);
		kryo.register(Tuple2.class);
		kryo.register(LinkedHashSet.class);
		kryo.register(Tuple2Serializable.class);
		kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
		kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());
		kryo.register(BlocksLocation.class);
		kryo.register(ReducerValues.class);
		kryo.register(RetrieveData.class);
		kryo.register(RetrieveKeys.class);
		kryo.register(Block.class);
		kryo.register(Block[].class);
		kryo.register(java.util.HashSet.class);
		kryo.register(byte[].class);
		kryo.register(ConcurrentHashMap.class);
		kryo.register(java.util.AbstractList.class);
		kryo.register(LoadJar.class);
		kryo.register(JobStage.class);
		kryo.register(RemoteDataFetch[].class);
		kryo.register(RemoteDataFetch.class);
		kryo.register(Stage.class);
		kryo.register(Task.class);
		kryo.register(DataCruncherContext.class);
		kryo.register(JSONObject.class);
		kryo.setRegistrationRequired(false);
		kryo.setReferences(true);
		log.debug("Exiting Utils.registerKryoResult");
	}

	/**
	 * This function returns the kryo object after configuring the 
	 * custom serializers.
	 * @return kryo
	 */
	public static Kryo getKryoResult() {
		log.debug("Entered Utils.getKryoResult");
		var kryo = new Kryo();
		registerKryoResult(kryo);
		log.debug("Exiting Utils.getKryoResult");
		return kryo;
	}

	/**
	 * This function returns the object by socket using the 
	 * host port of the server and the input object to the server.
	 * @param hp
	 * @param inputobj
	 * @return object
	 */
	public static Object getResultObjectByInput(String hp, Object inputobj) {
		var hostport = hp.split(MDCConstants.UNDERSCORE);
		try (var socket = new Socket(hostport[0], Integer.parseInt(hostport[1]));
				var input = new Input(socket.getInputStream());
				var output = new Output(socket.getOutputStream());) {
			var kryo = Utils.getKryoNonDeflateSerializer();
			writeObjectByStream(socket.getOutputStream(), inputobj);
			var obj = kryo.readClassAndObject(input);
			return obj;
		} catch (Exception ex) {
			log.error("Unable to read result Object: " + inputobj+" "+hp, ex);
		}
		return null;
	}

	/**
	 * This function returns the objects by socket.
	 * @param socket
	 * @return object
	 */
	public static Object readObject(Socket socket) {
		try {
			var input = new Input(socket.getInputStream());
			var kryo = Utils.getKryoNonDeflateSerializer();
			return kryo.readClassAndObject(input);

		} catch (Exception ex) {
			log.error("Unable to read Object: ", ex);
		}
		return null;
	}
	/**
	 * This function returns the objects by socket and classloader. 
	 * @param socket
	 * @param cl
	 * @return object
	 */
	public static Object readObject(Socket socket,ClassLoader cl) {
		try {
			var input = new Input(socket.getInputStream());
			var kryo = Utils.getKryoNonDeflateSerializer();
			kryo.setClassLoader(cl);
			return kryo.readClassAndObject(input);

		} catch (Exception ex) {
			log.error("Unable to read Object: ", ex);
		}
		return null;
	}
	
	
	/**
	 * This method writes the object via socket connection to the server.
	 * @param socket
	 * @param object
	 */
	public static void writeObject(Socket socket, Object object) {
		try (var output = new Output(socket.getOutputStream());) {
			var kryo = Utils.getKryoNonDeflateSerializer();
			kryo.writeClassAndObject(output, object);
			output.flush();
		} catch (Exception ex) {
			log.error("Unable to write Object: ", ex);
		}
	}

	/**
	 * This method writes the object via socket connection 
	 * to the host and port of server.
	 * @param hp
	 * @param inputobj
	 * @throws Exception
	 */
	public static void writeObject(String hp, Object inputobj) throws Exception {
		var hostport = hp.split(MDCConstants.UNDERSCORE);
		try (var socket = new Socket(hostport[0], Integer.parseInt(hostport[1]));) {

			writeObjectByStream(socket.getOutputStream(), inputobj);
		} catch (IOException ex) {
			log.error("Unable to write Object: " + inputobj);
			throw ex;
		}
		catch (Exception ex) {
			log.error("Unable to write Object: " + inputobj);
		}
	}
	/**
	 * This method writes the object via sockets outputstream 
	 * of server.
	 * @param hp
	 * @param inputobj
	 * @throws Exception
	 */
	public static void writeObjectByStream(OutputStream ostream, Object inputobj) {
		try {
			var output = new Output(ostream);
			var kryo = Utils.getKryoNonDeflateSerializer();
			kryo.writeClassAndObject(output, inputobj);
			output.flush();
		} catch (Exception ex) {
			log.error("Unable to write Object Stream: " + inputobj);
		}
	}
	
	/**
	 * 
	 * @param bindaddr
	 * @return jgroups channel object
	 */
	public static JChannel getChannelWithPStack(String bindaddr) {
		try {
			var channel = new JChannel(System.getProperty(MDCConstants.USERDIR)+MDCConstants.BACKWARD_SLASH+MDCProperties.get().getProperty(MDCConstants.JGROUPSCONF));
			return channel;
		} catch (Exception ex) {
			log.error("Unable to add Protocol Stack: ",ex);
		}
		return null;
	}
	
	
	public static JChannel getChannelTSSHA(String bindaddress,Receiver receiver) throws Exception {
		JChannel channel = getChannelWithPStack(
				NetworkUtil.getNetworkAddress(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULERSTREAM_HOST)));
		channel.setName(bindaddress);
		channel.setDiscardOwnMessages(true);
		if(!Objects.isNull(receiver)) {
			channel.setReceiver(receiver);
		}
		channel.connect(MDCConstants.TSSHA);
		return channel;
	}
	
	
	public static List<String> getAllFilePaths(List<Path> paths){
		return paths.stream().map(path->path.toUri().toString()).collect(Collectors.toList());
	}
	
	public static long getTotalLengthByFiles(FileSystem hdfs ,List<Path> paths) throws IOException {
		long totallength = 0;
		for (var filepath : paths) {
			var fs = (DistributedFileSystem) hdfs;
			var dis = fs.getClient().open(filepath.toUri().getPath());
			totallength += dis.getFileLength();
			dis.close();
		}
		return totallength;
	}
	
	public static void createJar(File folder,String outputfolder,String outjarfilename)  {
		var manifest = new Manifest();
		manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
		try(var target = new JarOutputStream(new FileOutputStream(outputfolder+MDCConstants.BACKWARD_SLASH+outjarfilename), manifest);){
			add(folder, target);
		}
		catch (IOException ioe) {
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
				if (count == -1)
					break;
				target.write(buffer, 0, count);
			}
			target.closeEntry();
		} finally {
			if (in != null)
				in.close();
		}
	}
	
	public static String getUniqueID() {
		return UUID.randomUUID().toString();
	}
	
	public static ServerCnxnFactory startZookeeperServer(int clientport,int numconnections,int ticktime) throws Exception {
		var dataDirectory = System.getProperty("java.io.tmpdir");
		var dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();
		var server = new ZooKeeperServer(dir, dir, ticktime);
		ServerCnxnFactory scf = ServerCnxnFactory.createFactory(new InetSocketAddress(clientport), numconnections);
		scf.startup(server);
		return scf;
	}
	
	
	public static boolean getZGCMemUsage(float percentage) {
		MemoryUsage musage = mpBeanLocalToJVM.getCollectionUsage();
		float memusage = (float) (musage.getUsed()/(float)musage.getMax()*100.0);
		return memusage>=percentage;
	}
	
	public static void writeResultToHDFS(String hdfsurl, String filepath, InputStream is) throws Exception {
		try (var hdfs = FileSystem.get(new URI(hdfsurl), new Configuration());
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(hdfsurl + filepath),
						Short.parseShort(MDCProperties.get().getProperty(MDCConstants.DFSOUTPUTFILEREPLICATION,
								MDCConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)))));
				Input input = new Input(is)) {
			Kryo kryo = getKryoNonDeflateSerializer();
			while (input.available() > 0) {
				Object result = kryo.readClassAndObject(input);
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
			log.error(MassiveDataPipelineConstants.FILEIOERROR, e);
			throw new Exception(MassiveDataPipelineConstants.FILEIOERROR, e);
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
			log.error(MassiveDataPipelineConstants.FILEIOERROR, e);
			throw new Exception(MassiveDataPipelineConstants.FILEIOERROR, e);
		}
	}
	public static String getIntermediateInputStreamRDF(RemoteDataFetch rdf) throws Exception {
		log.debug("Entered Utils.getIntermediateInputStreamRDF");
		var path = (rdf.jobid + MDCConstants.HYPHEN +
				rdf.stageid + MDCConstants.HYPHEN +rdf.taskid
				+ MDCConstants.DATAFILEEXTN);
		log.debug("Returned Utils.getIntermediateInputStreamRDF");
		return path;
	}
	
	public static String getIntermediateInputStreamTask(Task task) throws Exception {
		log.debug("Entered Utils.getIntermediateInputStreamTask");
		var path = (task.jobid + MDCConstants.HYPHEN +
				task.stageid + MDCConstants.HYPHEN +task.taskid
				+ MDCConstants.DATAFILEEXTN);
		log.debug("Returned Utils.getIntermediateInputStreamTask");
		return path;
	}
	
	
	@SuppressWarnings("unchecked")
	public static String launchContainers(Integer numberofcontainers){
		var containerid = MDCConstants.CONTAINER+MDCConstants.HYPHEN+Utils.getUniqueID();
		var jobid = MDCConstants.JOB+MDCConstants.HYPHEN+Utils.getUniqueID();
		var ac = new AllocateContainers();
		ac.setContainerid(containerid);
		ac.setNumberofcontainers(numberofcontainers);
		var nrs = MDCNodesResourcesSnapshot.get();		
		var resources = nrs.values();
		int numavailable = Math.min(numberofcontainers, resources.size());
		Iterator<Resources> res = resources.iterator();
		var lcs = new ArrayList<LaunchContainers>();
		for(int container=0;container<numavailable;container++) {
			Resources restolaunch = res.next();
			List<Integer> ports = (List<Integer>) Utils.getResultObjectByInput(restolaunch.getNodeport(), ac);
			log.info("Container Allocated In Node: "+restolaunch.getNodeport()+" With Ports: "+ports);
			var cla = new ContainerLaunchAttributes();
			var crs = new ContainerResources();
			crs.setPort(ports.get(0));
			crs.setCpu(restolaunch.getNumberofprocessors());
			var meminmb = restolaunch.getFreememory()/MDCConstants.MB;
			var heapmem = meminmb*30/100;
			crs.setMinmemory(heapmem);
			crs.setMaxmemory(heapmem);
			crs.setDirectheap(meminmb-heapmem);
			crs.setGctype(MDCConstants.ZGC);
			cla.setCr(Arrays.asList(crs));
			cla.setNumberofcontainers(1);
			LaunchContainers lc = new LaunchContainers();
			lc.setCla(cla);
			lc.setNodehostport(restolaunch.getNodeport());
			lc.setContainerid(containerid);
			lc.setJobid(jobid);
			List<Integer> launchedcontainerports = (List<Integer>) Utils.getResultObjectByInput(lc.getNodehostport(), lc);			
			int index = 0;
			while (index < launchedcontainerports.size()) {
				while (true) {
					String tehost = lc.getNodehostport().split("_")[0];
					try (var sock = new Socket(tehost, launchedcontainerports.get(index));) {
						break;
					} catch (Exception ex) {
						try {
							log.info("Waiting for container "+ tehost+MDCConstants.UNDERSCORE+launchedcontainerports.get(index)+ " to complete launch....");
							Thread.sleep(1000);
						} catch (Exception e) {
						}
					}
				}
				index++;
			}
			log.info("Container Launched In Node: "+restolaunch.getNodeport()+" With Ports: "+launchedcontainerports);
			lcs.add(lc);
		}
		GlobalContainerLaunchers.put(containerid, lcs);
		return containerid;
	}
	
	
	public static void destroyContainers(String containerid) {
		var dc = new DestroyContainers();
		dc.setContainerid(containerid);
		var lcs = GlobalContainerLaunchers.get(containerid);
		lcs.stream().forEach(lc->{
			try {
				Utils.writeObject(lc.getNodehostport(), dc);
			} catch (Exception e) {
				log.error(MDCConstants.EMPTY,e);
			}
		});
		GlobalContainerLaunchers.remove(containerid);
	}
	
	
	
}
