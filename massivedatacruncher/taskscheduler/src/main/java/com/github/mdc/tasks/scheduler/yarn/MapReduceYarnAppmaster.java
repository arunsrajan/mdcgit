package com.github.mdc.tasks.scheduler.yarn;

import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.StaticEventingAppmaster;
import org.springframework.yarn.am.allocate.DefaultContainerAllocator;
import org.springframework.yarn.am.container.AbstractLauncher;

import com.github.mdc.common.ApplicationTask;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.ByteBufferPool;
import com.github.mdc.common.ByteBufferPoolDirect;
import com.github.mdc.common.Context;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.Tuple2Serializable;
import com.github.mdc.tasks.scheduler.JobConfiguration;
import com.google.common.collect.Iterables;

/**
 * 
 * @author Arun
 * Yarn App master with lifecycle init, submitapplication, isjobcomplete and prelaunch containers.
 * Various container events captured are container failure and completed operation 
 * with container statuses.
 */
public class MapReduceYarnAppmaster extends StaticEventingAppmaster implements ContainerLauncherInterceptor {

	private static final Log log = LogFactory.getLog(MapReduceYarnAppmaster.class);
	
	private Map<String,Object> pendingjobs = new ConcurrentHashMap<>();
	private final Semaphore lock = new Semaphore(1);
	@SuppressWarnings("rawtypes")
	private DataCruncherContext dcc = new DataCruncherContext();
	private long taskcompleted = 0, redtaskcompleted = 0;
	private int tasksubmitted = 0;
	private int redtasksubmitted = 0;
	private int numreducers;
	/**
	 * Container initialization.
	 */
	@Override
	protected void onInit() throws Exception {
		super.onInit();
		if(getLauncher() instanceof AbstractLauncher launcher) {
			launcher.addInterceptor(this);
		}
	}
	Map<String, Set<String>> mapclzchunkfile;
	Set<String> combiner;
	Set<String> reducer;
	Map<String, List<BlocksLocation>> folderfileblocksmap;
	List<YarnReducer> rs = new ArrayList<>();
	JobConfiguration jobconf;
	Map<String,List<MapperCombiner>> ipmcs = new ConcurrentHashMap<>();
	int totalmappersize;
	Map<String,String> containeridipmap = new ConcurrentHashMap<>();
	Map<String,Integer> iptasksubmittedmap = new ConcurrentHashMap<>();
	/**
	 * Submit the user application. The various parameters obtained from
	 * HDFS are graph with node and edges, job stage map, job stage with 
	 * task information.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void submitApplication() {
		try {
			ByteBufferPoolDirect.init();
			ByteBufferPool.init(3);
			log.info("Environment: "+getEnvironment());
			var yarninputfolder = MDCConstants.YARNINPUTFOLDER+MDCConstants.BACKWARD_SLASH+getEnvironment().get(MDCConstants.YARNMDCJOBID);
			log.info("Yarn Input Folder: "+yarninputfolder);
			log.info("AppMaster HDFS: "+getConfiguration().get(MDCConstants.HDFSNAMENODEURL));
			var containerallocator = (DefaultContainerAllocator) getAllocator();
			log.info("Parameters: "+getParameters() );
			log.info("Container-Memory: "+getParameters().getProperty("container-memory", "1024"));
			containerallocator.setMemory(Integer.parseInt(getParameters().getProperty("container-memory", "1024")));
			System.setProperty(MDCConstants.HDFSNAMENODEURL, getConfiguration().get(MDCConstants.HDFSNAMENODEURL));
			//Thread containing the job stage information.
			mapclzchunkfile = (Map<String, Set<String>>) RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS( yarninputfolder,
					MDCConstants.MASSIVEDATA_YARNINPUT_MAPPER);
			combiner = (Set<String>) RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS( yarninputfolder,
					MDCConstants.MASSIVEDATA_YARNINPUT_COMBINER);
			reducer = (Set<String>) RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS(yarninputfolder,
					MDCConstants.MASSIVEDATA_YARNINPUT_REDUCER);
			folderfileblocksmap = (Map<String, List<BlocksLocation>>) RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS(yarninputfolder,
					MDCConstants.MASSIVEDATA_YARNINPUT_FILEBLOCKS);
			jobconf = (JobConfiguration) RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS(yarninputfolder,
					MDCConstants.MASSIVEDATA_YARNINPUT_CONFIGURATION);
			numreducers = Integer.parseInt(jobconf.getNumofreducers());
		}
		catch(Exception ex) {
			log.info("Submit Application Error, See cause below \n",ex);
		}
		var appmasterservice = (MapReduceYarnAppmasterService) getAppmasterService();
		log.info("In SubmitApplication Setting AppMaster Service: "+appmasterservice);
		if(appmasterservice!=null) {
			//Set the Yarn App master bean to the Yarn App master service object. 
			appmasterservice.setYarnAppMaster(this);
		}
		var taskcount = 0;
		var applicationid = MDCConstants.MDCAPPLICATION + MDCConstants.HYPHEN + System.currentTimeMillis();
		var bls = folderfileblocksmap.keySet().stream()
				.flatMap(key -> folderfileblocksmap.get(key).stream()).collect(Collectors.toList());
		List<MapperCombiner> mappercombiners;
		totalmappersize = bls.size();
		for (var bl : bls) {
			var taskid = MDCConstants.TASK + MDCConstants.HYPHEN + MDCConstants.MAPPER + MDCConstants.HYPHEN
					+ (taskcount + 1);
			var apptask = new ApplicationTask();
			apptask.applicationid = applicationid;
			apptask.taskid = taskid;
			var mc = (MapperCombiner) getMapperCombiner(mapclzchunkfile, combiner, bl, apptask);
			var xrefdnaddrs = new ArrayList<>(bl.block[0].dnxref.keySet());
			var key = xrefdnaddrs.get(taskcount%xrefdnaddrs.size());
			var dnxrefaddr = new ArrayList<>(bl.block[0].dnxref.get(key));
			bl.block[0].hp = dnxrefaddr.get(taskcount%dnxrefaddr.size());
			var host = bl.block[0].hp.split(":")[0];
			if(Objects.isNull(ipmcs.get(host))) {
				mappercombiners = new ArrayList<>();
				ipmcs.put(host, mappercombiners);
			}else {
				mappercombiners = ipmcs.get(host);
			}
			mappercombiners.add(mc);
			taskcount++;
		}
		log.info(bls);
		taskcount = 0;
		while (taskcount < numreducers) {
			var red = new YarnReducer();
			red.reducerclasses = reducer;
			var taskid =  MDCConstants.TASK + MDCConstants.HYPHEN + MDCConstants.REDUCER + MDCConstants.HYPHEN
					+ (taskcount + 1);
			var apptask = new ApplicationTask();
			apptask.applicationid = applicationid;
			apptask.taskid = taskid;
			red.apptask = apptask;
			rs.add(red);
			taskcount++;
		}
		log.info(rs);
		var prop = new Properties();
		prop.put(MDCConstants.HDFSNAMENODEURL, getConfiguration().get(MDCConstants.HDFSNAMENODEURL));
		MDCProperties.put(prop);
		super.submitApplication();
	}

	
	
	public MapperCombiner getMapperCombiner(
			Map<String, Set<String>> mapclzchunkfile,
			Set<String> combiners, BlocksLocation blockslocation,ApplicationTask apptask){
		var rawpath = blockslocation.block[0].filename.split(MDCConstants.BACKWARD_SLASH);
		var mapcombiner = new MapperCombiner(blockslocation, mapclzchunkfile.get(MDCConstants.BACKWARD_SLASH + rawpath[3]), apptask,
				combiners);
		return mapcombiner;
	}
	
	/**
	 * Set App Master service hosts and port running before the container is launched.
	 */
	@Override
	public ContainerLaunchContext preLaunch(Container container, ContainerLaunchContext context) {
		var service = getAppmasterService();
		if (service != null) {
			Map<String, String> env = null;
			try {
				containeridipmap.put(container.getId().toString().trim(), container.getNodeId().getHost());
				var port = service.getPort();
				var address = InetAddress.getLocalHost().getHostAddress();
				log.info("preLaunch Container Id Ip Map:"+containeridipmap);
				log.info("App Master Service Ip Address: "+address);
				log.info("App Master Service Port: "+port);
				log.info("Container Id: "+container.getId().toString()+" Ip: "+service.getHost());
				env = new HashMap<>(context.getEnvironment());
				//Set the service port to the environment object.
				env.put(YarnSystemConstants.AMSERVICE_PORT, Integer.toString(port));
			
				//Set the service host to the environment object.
				env.put(YarnSystemConstants.AMSERVICE_HOST, address);
			} catch (Exception ex) {
				log.info("Container Prelaunch error, See cause below \n",ex);
			}
			context.setEnvironment(env);
			return context;
		} else {
			return context;
		}
	}
	/**
	 * Execute the OnContainer completed method when container is exited with
	 * the exitcode.
	 */
	@Override
	protected void onContainerCompleted(ContainerStatus status) {
		try {
			lock.acquire();
			super.onContainerCompleted(status);
			log.info("Container completed: "+status.getContainerId());
		}
		catch (InterruptedException e) {
			log.warn("Interrupted!", e);
		    // Restore interrupted state...
		    Thread.currentThread().interrupt();
		}catch(Exception ex) {
			log.info("Container Completion fails",ex);
		}
		finally {
			lock.release();
		}
	}
		
	/**
	 * Execute the OnContainer failed method when container is exited with
	 * the exitcode.
	 */
	@Override
	protected boolean onContainerFailed(ContainerStatus status) {
		try {
			lock.acquire();
			log.info("Container failed: "+status.getContainerId());
			return true;
		}
		catch (InterruptedException e) {
			log.warn("Interrupted!", e);
		    // Restore interrupted state...
		    Thread.currentThread().interrupt();
		    return false;
		} catch(Exception ex) {
			log.info("Container allocation fails",ex);
			return false;
		}
		finally {
			lock.release();
		}
	}
	

	/**
	 * Update the job statuses if job status is completed.
	 * @param job
	 * @param success
	 */
	@SuppressWarnings("unchecked")
	public void reportJobStatus(MapperCombiner mc, boolean success,String containerid) {
		try {
			lock.acquire();
			if (success) {
				log.info(mc.apptask.applicationid+mc.apptask.taskid+" Updated");
				var keys = (Set<Object>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(mc.apptask.applicationid,
						(mc.apptask.applicationid + mc.apptask.taskid + MDCConstants.DATAFILEEXTN), true);
				dcc.putAll(keys,mc.apptask.applicationid + mc.apptask.taskid);
				log.info("dcc: "+dcc);
				pendingjobs.remove(mc.apptask.applicationid+mc.apptask.taskid);
				taskcompleted++;
			} else {
				pendingjobs.put(mc.apptask.applicationid+mc.apptask.taskid,mc);
			}
		}
		catch (InterruptedException e) {
			log.warn("Interrupted!", e);
		    // Restore interrupted state...
		    Thread.currentThread().interrupt();
		} catch(Exception ex) {
			log.info("reportJobStatus fails",ex);
		}
		finally {
			lock.release();
		}
	}
	
	
	public void reportJobStatus(YarnReducer r, boolean success,String containerid) {
		try {
			lock.acquire();
			if (success) {
				if(redtaskcompleted+1==rs.size()) {
					var sb = new StringBuilder();
					for (var redcount = 0; redcount < numreducers; redcount++) {
						var red = rs.get(redcount);
						var ctxreducerpart = (Context) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(
								red.apptask.applicationid,
								(red.apptask.applicationid + red.apptask.taskid + MDCConstants.DATAFILEEXTN), false);
						var keysreducers = ctxreducerpart.keys();
						sb.append(MDCConstants.NEWLINE);
						sb.append("Partition "+(redcount+1)+"-------------------------------------------------");
						sb.append(MDCConstants.NEWLINE);
						for (var key : keysreducers) {
							sb.append(key + MDCConstants.SINGLESPACE + ctxreducerpart.get(key));
							sb.append(MDCConstants.NEWLINE);
						}
						sb.append("-------------------------------------------------");
						sb.append(MDCConstants.NEWLINE);
						sb.append(MDCConstants.NEWLINE);
					}
					var filename = MDCConstants.MAPRED + MDCConstants.HYPHEN + System.currentTimeMillis();
					log.info("Writing Results to file: " + filename);
					try (var hdfs = FileSystem.get(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
							new Configuration());var fsdos = hdfs.create(new Path(
							MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL) + MDCConstants.BACKWARD_SLASH
									+ jobconf.getOutputfolder() + MDCConstants.BACKWARD_SLASH + filename));) {
						fsdos.write(sb.toString().getBytes());
					} catch (Exception ex) {
						log.error(MDCConstants.EMPTY, ex);
					}
				}
				log.info(r.apptask.applicationid+r.apptask.taskid+" Updated");
				pendingjobs.remove(r.apptask.applicationid+r.apptask.taskid);
				redtaskcompleted++;
			} else {
				pendingjobs.put(r.apptask.applicationid+r.apptask.taskid,r);
			}
		}
		catch (InterruptedException e) {
			log.warn("Interrupted!", e);
		    // Restore interrupted state...
		    Thread.currentThread().interrupt();
		}
		catch(Exception ex) {
			log.info("reportJobStatus fails",ex);
		}
		finally {
			lock.release();
		}
	}
	
	Iterator<List<Tuple2Serializable>> partkeys;
	/**
	 * Obtain the job to execute
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Object getJob(String containerid) {
		try {
			lock.acquire();
			if (!pendingjobs.keySet().isEmpty()) {
				return pendingjobs.remove(pendingjobs.keySet().iterator().next());
			} else if (tasksubmitted < totalmappersize) {
				log.info("getJob Container Id Ip Map:"+containeridipmap);
				var ip = containeridipmap.get(containerid.trim());
				var iptasksubmitted = iptasksubmittedmap.get(ip)==null?0:iptasksubmittedmap.get(ip);
				var mcs = ipmcs.get(ip);
				if(Objects.isNull(mcs)) {
					return null;
				}
				if(iptasksubmitted<mcs.size()) {
					var mc = ipmcs.get(ip).get(iptasksubmitted++);
					iptasksubmittedmap.put(ip, iptasksubmitted);
					tasksubmitted++;
					return mc;
				}
			} else if (redtasksubmitted < rs.size() && taskcompleted >= totalmappersize) {
				if(redtasksubmitted==0) {
					var keyapptasks = (List<Tuple2Serializable>) dcc.keys().parallelStream()
							.map(key -> new Tuple2Serializable(key, dcc.get(key)))
							.collect(Collectors.toCollection(ArrayList::new));
					partkeys = Iterables
							.partition(keyapptasks, (keyapptasks.size()) / numreducers).iterator();
				}
				rs.get(redtasksubmitted).tuples = partkeys.next();
				log.info("Tuples: "+rs.get(redtasksubmitted).tuples);
				return rs.get(redtasksubmitted++);
			}
			return null;
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
		    // Restore interrupted state...
		    Thread.currentThread().interrupt();
		    return null;
		} catch (Exception ex) {
			log.info("reportJobStatus fails", ex);
			return null;
		} finally {
			lock.release();
		}
	}

	/**
	 * Check on whether the jobs are available to execute.
	 * @return
	 */
	public boolean hasJobs() {
		try {
			lock.acquire();
			return (pendingjobs.size()>0||taskcompleted<totalmappersize||redtaskcompleted<rs.size());
		}
		catch (InterruptedException e) {
			log.warn("Interrupted!", e);
		    // Restore interrupted state...
		    Thread.currentThread().interrupt();
		    return false;
		} catch(Exception ex) {
			log.info("hasJobs fails",ex);
			return false;
		}
		finally {
			lock.release();
		}
	}
	
	
	@Override
	public String toString() {
		return MDCConstants.PENDINGJOBS+MDCConstants.EQUAL + pendingjobs.size() + MDCConstants.SINGLESPACE+MDCConstants.RUNNINGJOBS+MDCConstants.EQUAL+ pendingjobs.size();
	}
}
