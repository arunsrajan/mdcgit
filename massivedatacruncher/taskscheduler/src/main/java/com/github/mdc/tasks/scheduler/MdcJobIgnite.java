package com.github.mdc.tasks.scheduler;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.log4j.Logger;
import org.xerial.snappy.SnappyOutputStream;

import com.github.dexecutor.core.DefaultDexecutor;
import com.github.dexecutor.core.DexecutorConfig;
import com.github.dexecutor.core.ExecutionConfig;
import com.github.dexecutor.core.task.TaskProvider;
import com.github.mdc.common.BlockExecutors;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.Context;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.HDFSBlockUtils;
import com.github.mdc.common.HdfsBlockReader;
import com.github.mdc.common.HeartBeatTaskScheduler;
import com.github.mdc.common.JobMetrics;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCJobMetrics;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.utils.FileBlocksPartitionerHDFS;
import com.github.mdc.tasks.executor.CrunchCombiner;
import com.github.mdc.tasks.executor.CrunchMapper;
import com.github.mdc.tasks.executor.CrunchReducer;
import com.github.mdc.tasks.scheduler.ignite.MapReduceResult;
import com.github.mdc.tasks.scheduler.ignite.MassiveDataCruncherMapperCombiner;
import com.github.mdc.tasks.scheduler.ignite.MassiveDataCruncherReducer;

@SuppressWarnings("rawtypes")
public class MdcJobIgnite implements Callable<List<DataCruncherContext>> {
	String jobname;
	JobConfiguration jobconf;
	protected List<MapperInput> mappers;
	protected List<Class<?>> combiners;
	protected List<Class<?>> reducers;
	String outputfolder;
	int batchsize;
	int numreducers;
	public String hdfsdirpath;
	FileSystem hdfs;
	List<Path> blockpath = new ArrayList<>();
	int totalreadsize = 0;
	byte[] read1byt = new byte[1];
	int blocksize;
	Path currentfilepath;
	int blocklocationindex = 0;
	long redcount = 0;
	List<BlocksLocation> bls;
	List<String> nodes;
	CuratorFramework cf;
	static Logger log = Logger.getLogger(MdcJobIgnite.class);
	Set<BlockExecutors> locations;
	List<LocatedBlock> locatedBlocks;
	Collection<String> locationsblock;
	int executorindex = 0;
	ExecutorService es;
	public MdcJobIgnite(String jobname, JobConfiguration jobconf, List<MapperInput> mappers,
			List<Class<?>> combiners, List<Class<?>> reducers, String outputfolder) {
		this.jobname = jobname;
		this.jobconf = jobconf;
		this.mappers = mappers;
		this.combiners = combiners;
		this.reducers = reducers;
		this.outputfolder = outputfolder;
	}

	HeartBeatTaskScheduler hbts;
	List<MapReduceResult> mrresults = new ArrayList<>();
	Map<String, ArrayBlockingQueue> containerqueue = new ConcurrentHashMap<>();
	List<Integer> ports = null;
	protected List<String> containers;
	protected List<String> nodessorted;
	private Semaphore semaphore;
	Ignite ignite;
	IgniteCache<Object, byte[]> ignitecache;
	IgniteCache<Object,DataCruncherContext> cachemr;
	@SuppressWarnings("unchecked")
	public List<DataCruncherContext> call() {
		try {
			var starttime = System.currentTimeMillis();
			batchsize = Integer.parseInt(jobconf.getBatchsize());
			numreducers = Integer.parseInt(jobconf.getNumofreducers());
			var configuration = new Configuration();
			blocksize = Integer.parseInt(jobconf.getBlocksize());
			hdfs = FileSystem.get(new URI(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HDFSNN)),
					configuration);
			var kryo = Utils.getKryoNonDeflateSerializer();
			var combiner = new HashSet<>();
			var reducer = new HashSet<>();
			var mapclzchunkfile = new HashMap<String, Set<Class>>();
			hdfsdirpath = MDCConstants.EMPTY;
			semaphore = new Semaphore(Integer.parseInt(jobconf.getBatchsize()));
			var hdfsdirpaths = new LinkedHashSet<String>();
			for (var mapperinput : mappers) {
				try {
					if (mapclzchunkfile.get(mapperinput.inputfolderpath) == null) {
						mapclzchunkfile.put(mapperinput.inputfolderpath, new HashSet<>());
					}
					mapclzchunkfile.get(mapperinput.inputfolderpath).add(mapperinput.crunchmapper);
					hdfsdirpaths.add(mapperinput.inputfolderpath);
				} catch (Error ex) {

				}
			}
			
			var mrtaskcount = 0;
			var jm = new JobMetrics();
			jm.jobstarttime = System.currentTimeMillis();
			jm.jobid = MDCConstants.MDCAPPLICATION + MDCConstants.HYPHEN + System.currentTimeMillis();
			MDCJobMetrics.put(jm);
			var cfg = new IgniteConfiguration();
			// The node will be started as a client node.
			cfg.setClientMode(true);
			cfg.setDeploymentMode(DeploymentMode.CONTINUOUS);
			// Classes of custom Java logic will be transferred over the wire from
			// this app.
			cfg.setPeerClassLoadingEnabled(true);
			// Setting up an IP Finder to ensure the client can locate the servers.
			var ipFinder = new TcpDiscoveryMulticastIpFinder();
			ipFinder.setMulticastGroup(jobconf.getIgnitemulticastgroup());
			cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));
			var cc = new CacheConfiguration(MDCConstants.MDCCACHE);
			cc.setCacheMode(CacheMode.PARTITIONED);
			cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
			cc.setBackups(Integer.parseInt(jobconf.getIgnitebackup()));
			cfg.setCacheConfiguration(cc);
			// Starting the node
			ignite = Ignition.start(cfg);
			ignitecache = ignite.getOrCreateCache(MDCConstants.MDCCACHE);
			var mdcmcs = new ArrayList<MassiveDataCruncherMapperCombiner>();
			var allfiles = new ArrayList<String>();
			var folderfileblocksmap = new ConcurrentHashMap<>();
			boolean isblocksuserdefined = Boolean.parseBoolean(jobconf.getIsblocksuserdefined());
			for (var hdfsdir : hdfsdirpaths) {
				var fileStatus = hdfs.listStatus(
						new Path(MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HDFSNN) + hdfsdir));
				var paths = FileUtil.stat2Paths(fileStatus);
				blockpath.addAll(Arrays.asList(paths));
				allfiles.addAll(Utils.getAllFilePaths(blockpath));
				jm.totalfilesize += Utils.getTotalLengthByFiles(hdfs, blockpath);
				bls = new ArrayList<>();				
				if(isblocksuserdefined) {
					bls.addAll(HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, blockpath, isblocksuserdefined, blocksize * MDCConstants.MB));
				}else {
					bls.addAll(HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, blockpath, isblocksuserdefined, 128 * MDCConstants.MB));
				}
				folderfileblocksmap.put(hdfsdir, bls);
				FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
				fbp.getDnXref(bls, false);
				for(var bl:bls) {
					var databytes = HdfsBlockReader.getBlockDataMR(bl, hdfs);
					var baos = new ByteArrayOutputStream();
					var lzfos = new SnappyOutputStream(baos);
					lzfos.write(databytes);
					lzfos.flush();
					ignitecache.put(bl, baos.toByteArray());
					lzfos.close();
					for (var mapperinput : mapclzchunkfile.get(hdfsdir)) {
						var mdcmc = new MassiveDataCruncherMapperCombiner(bl,(List<CrunchMapper>)Arrays.asList((CrunchMapper)mapperinput.newInstance()),
								(List<CrunchCombiner>)Arrays.asList((CrunchCombiner)combiners.get(0).newInstance()));
						mdcmcs.add(mdcmc);
					}
				}
				blockpath.clear();
			}
			jm.totalfilesize = jm.totalfilesize/MDCConstants.MB;
			jm.files = allfiles;
			jm.mode = jobconf.execmode;
			jm.totalblocks = bls.size();
			log.debug("Total MapReduce Tasks: " + mdcmcs.size());
			
			for (var cls : combiners) {
				if (cls != null) {
					combiner.add(cls.getName());
				}
			}
			for (var cls : reducers) {
				if (reducer != null) {
					reducer.add(cls.getName());
				}
			}
			
			DexecutorConfig<MassiveDataCruncherMapperCombiner, Boolean> configmc = new DexecutorConfig(newExecutor(),
					new TaskProviderIgniteMapperCombiner());
			DefaultDexecutor<MassiveDataCruncherMapperCombiner, Boolean> executormc = new DefaultDexecutor<>(configmc);

			for (var mdcmc : mdcmcs) {
				executormc.addDependency(mdcmc, mdcmc);
			}
			executormc.execute(ExecutionConfig.NON_TERMINATING);
			log.debug("Waiting for the Reducer to complete------------");
			var dccctx = new DataCruncherContext();
			cachemr = ignite.getOrCreateCache(MDCConstants.MDCCACHEMR);
			for(var mrresult:mrresults) {
				var ctx = (Context) cachemr.get(mrresult.cachekey);
				dccctx.add(ctx);
			}
			var executorser = Executors.newWorkStealingPool();
			var ctxes = new ArrayList<Future<Context>>();
			var result = new ArrayList<DataCruncherContext>();
			var mdcr = new MassiveDataCruncherReducer(dccctx,(CrunchReducer) reducers.get(0).newInstance());
			ctxes.add(executorser.submit(mdcr));
			for(var res:ctxes) {
				result.add((DataCruncherContext) res.get());
			}
			
			log.debug("Reducer completed------------------------------");
			var sb = new StringBuilder();
			var partindex = 1;
			for(var ctxreducerpart:result) {
				var keysreducers = ctxreducerpart.keys();
				sb.append(MDCConstants.NEWLINE);
				sb.append("Partition "+partindex+"-------------------------------------------------");
				sb.append(MDCConstants.NEWLINE);
				for (var key : keysreducers) {
					sb.append(key + MDCConstants.SINGLESPACE + ctxreducerpart.get(key));
					sb.append(MDCConstants.NEWLINE);
				}
				sb.append("-------------------------------------------------");
				sb.append(MDCConstants.NEWLINE);
				sb.append(MDCConstants.NEWLINE);
				partindex++;
			}
			var filename = MDCConstants.MAPRED + MDCConstants.HYPHEN + System.currentTimeMillis();
			log.debug("Writing Results to file: " + filename);
			try (var fsdos = hdfs.create(new Path(
					MDCProperties.get().getProperty(MDCConstants.TASKSCHEDULER_HDFSNN) + MDCConstants.BACKWARD_SLASH
							+ this.outputfolder + MDCConstants.BACKWARD_SLASH + filename));) {
				fsdos.write(sb.toString().getBytes());
			} catch (Exception ex) {
				log.error(MDCConstants.EMPTY, ex);
			}
			jm.jobcompletiontime = System.currentTimeMillis();
			jm.totaltimetaken = (jm.jobcompletiontime - jm.jobstarttime) /1000.0;
			if(!Objects.isNull(jobconf.getOutput())) {
				Utils.writeKryoOutput(kryo, jobconf.getOutput(),
					"Completed Job in " + (jm.totaltimetaken) + " seconds");
			}
			return result;
		} catch (Exception ex) {
			log.info("Unable To Execute Job, See Cause Below:", ex);
		} finally {
			if(!Objects.isNull(ignitecache)) {
				ignitecache.close();
			}
			if(!Objects.isNull(cachemr)) {
				cachemr.close();
			}
			if(!Objects.isNull(ignite)) {
				ignite.close();
			}
		}
		return null;
	}
	
	private ExecutorService newExecutor() {
		return Executors.newWorkStealingPool();
	}
	Semaphore resultsemaphore = new Semaphore(1);
	public class TaskProviderIgniteMapperCombiner
			implements TaskProvider<MassiveDataCruncherMapperCombiner, Boolean> {
		
		public com.github.dexecutor.core.task.Task<MassiveDataCruncherMapperCombiner, Boolean> provideTask(
				final MassiveDataCruncherMapperCombiner mdcmc) {

			return new com.github.dexecutor.core.task.Task<MassiveDataCruncherMapperCombiner, Boolean>() {

				private static final long serialVersionUID = 1L;

				public Boolean execute() {
					try {
						semaphore.acquire();
						var compute = ignite.compute(ignite.cluster().forServers());						
						var mrresult = compute.affinityCall(MDCConstants.MDCCACHE, mdcmc.getBlocksLocation(), mdcmc);
						resultsemaphore.acquire();
						mrresults.add(mrresult);
						log.info(mrresult);
						resultsemaphore.release();
						semaphore.release();
					} catch (Exception e) {
						log.error("TaskProviderIgnite error", e);
					}
					return true;
				}
			};
		}
	}
}
