package com.github.mdc.tasks.scheduler;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.yarn.client.CommandYarnClient;

import com.github.mdc.common.BlockExecutors;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.HDFSBlockUtils;
import com.github.mdc.common.HeartBeatServer;
import com.github.mdc.common.HeartBeatTaskScheduler;
import com.github.mdc.common.JobMetrics;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCJobMetrics;
import com.github.mdc.common.MDCNodesResources;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.RemoteDataFetcher;
import com.github.mdc.common.Utils;

@SuppressWarnings("rawtypes")
public class MapReduceApplicationYarn implements Callable<List<DataCruncherContext>> {
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
	int totalreadsize;
	byte[] read1byt = new byte[1];
	int blocksize;
	Path currentfilepath;
	int blocklocationindex;
	long redcount;
	List<BlocksLocation> bls;
	List<String> nodes;
	CuratorFramework cf;
	static Logger log = Logger.getLogger(MapReduceApplicationYarn.class);
	Set<BlockExecutors> locations;
	List<LocatedBlock> locatedBlocks;
	Collection<String> locationsblock;
	int executorindex;
	ExecutorService es;
	HeartBeatServer hbs;

	public MapReduceApplicationYarn(String jobname, JobConfiguration jobconf, List<MapperInput> mappers,
			List<Class<?>> combiners, List<Class<?>> reducers, String outputfolder) {
		this.jobname = jobname;
		this.jobconf = jobconf;
		this.mappers = mappers;
		this.combiners = combiners;
		this.reducers = reducers;
		this.outputfolder = outputfolder;
	}

	HeartBeatTaskScheduler hbts;

	Map<String, ArrayBlockingQueue> containerqueue = new ConcurrentHashMap<>();
	List<Integer> ports;
	protected List<String> containers;
	protected List<String> nodessorted;

	public void getDnXref(List<BlocksLocation> bls) throws MapReduceException {
		log.debug("Entered MdcJob.getDnXref");
		var dnxrefs = bls.stream().parallel().flatMap(bl -> {
			var xrefs = new LinkedHashSet<String>();
			Iterator<Set<String>> xref = bl.block[0].dnxref.values().iterator();
			for (; xref.hasNext(); ) {
				xrefs.addAll(xref.next());
			}
			if (bl.block.length > 1 && !Objects.isNull(bl.block[1])) {
				xref = bl.block[0].dnxref.values().iterator();
				for (; xref.hasNext(); ) {
					xrefs.addAll(xref.next());
				}
			}
			return xrefs.stream();
		}).collect(Collectors.groupingBy(key -> key.split(MDCConstants.COLON)[0],
				Collectors.mapping(xref -> xref, Collectors.toCollection(LinkedHashSet::new))));
		var dnxrefallocatecount = (Map<String, Long>) dnxrefs.keySet().stream().parallel().flatMap(key -> {
			return dnxrefs.get(key).stream();
		}).collect(Collectors.toMap(xref -> xref, xref -> 0l));

		for (var b : bls) {
			var xrefselected = b.block[0].dnxref.keySet().stream()
					.flatMap(xrefhost -> b.block[0].dnxref.get(xrefhost).stream()).sorted((xref1, xref2) -> {
				return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
			}).findFirst();
			var xref = xrefselected.get();
			dnxrefallocatecount.put(xref, dnxrefallocatecount.get(xref) + 1);
			b.block[0].hp = xref;
			if (b.block.length > 1 && !Objects.isNull(b.block[1])) {
				xrefselected = b.block[1].dnxref.keySet().stream()
						.flatMap(xrefhost -> b.block[1].dnxref.get(xrefhost).stream()).sorted((xref1, xref2) -> {
					return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
				}).findFirst();
				xref = xrefselected.get();
				dnxrefallocatecount.put(xref, dnxrefallocatecount.get(xref) + 1);
				b.block[1].hp = xref;
			}
		}

		log.debug("Exiting MdcJob.getDnXref");
	}

	public List<DataCruncherContext> call() {
		String applicationid = MDCConstants.MDCAPPLICATION + MDCConstants.HYPHEN + System.currentTimeMillis();
		try {
			var starttime = System.currentTimeMillis();
			batchsize = Integer.parseInt(jobconf.getBatchsize());
			numreducers = Integer.parseInt(jobconf.getNumofreducers());
			var configuration = new Configuration();
			blocksize = Integer.parseInt(jobconf.getBlocksize());
			var jm = new JobMetrics();
			jm.jobstarttime = System.currentTimeMillis();
			jm.jobid = applicationid;
			MDCJobMetrics.put(jm);
			hdfs = FileSystem.get(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
					configuration);
			var kryo = Utils.getKryoNonDeflateSerializer();
			var combiner = new HashSet<String>();
			var reducer = new HashSet<>();
			var mapclzchunkfile = new HashMap<String, Set<String>>();
			hdfsdirpath = MDCConstants.EMPTY;
			for (var mapperinput : mappers) {
				try {
					if (mapclzchunkfile.get(mapperinput.inputfolderpath) == null) {
						mapclzchunkfile.put(mapperinput.inputfolderpath, new HashSet<>());
					}
					mapclzchunkfile.get(mapperinput.inputfolderpath).add(mapperinput.crunchmapper.getName());
					hdfsdirpath += mapperinput.inputfolderpath + MDCConstants.COMMA;
				} catch (Error ex) {

				}
			}
			var hdfsdirpaths = hdfsdirpath.substring(0, hdfsdirpath.length() - 1).split(MDCConstants.COMMA);
			var mrtaskcount = 0;
			var folderfileblocksmap = new ConcurrentHashMap<>();
			var allfiles = new ArrayList<String>();
			boolean isblocksuserdefined = Boolean.parseBoolean(jobconf.getIsblocksuserdefined());
			for (var hdfsdir : hdfsdirpaths) {
				var fileStatus = hdfs.listStatus(
						new Path(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL) + hdfsdir));
				var paths = FileUtil.stat2Paths(fileStatus);
				blockpath.addAll(Arrays.asList(paths));
				bls = new ArrayList<>();
				if (isblocksuserdefined) {
					bls.addAll(HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, blockpath, isblocksuserdefined, blocksize * MDCConstants.MB));
				} else {
					bls.addAll(HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, blockpath, isblocksuserdefined, 128 * MDCConstants.MB));
				}
				getDnXref(bls);
				mrtaskcount += bls.size();
				folderfileblocksmap.put(hdfsdir, bls);
				allfiles.addAll(Utils.getAllFilePaths(blockpath));
				jm.totalfilesize += Utils.getTotalLengthByFiles(hdfs, blockpath);
				blockpath.clear();
			}

			jm.totalfilesize = jm.totalfilesize / MDCConstants.MB;
			jm.files = allfiles;
			jm.mode = jobconf.execmode;
			jm.totalblocks = bls.size();
			log.debug("Total MapReduce Tasks: " + mrtaskcount);
			for (var mapperinput : mappers) {
				try {
					if (mapclzchunkfile.get(mapperinput.inputfolderpath) == null) {
						mapclzchunkfile.put(mapperinput.inputfolderpath, new HashSet<>());
					}
					mapclzchunkfile.get(mapperinput.inputfolderpath).add(mapperinput.crunchmapper.getName());

				} catch (Error ex) {

				}
			}
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
			var yarninputfolder = MDCConstants.YARNINPUTFOLDER + MDCConstants.BACKWARD_SLASH + applicationid;
			var output = jobconf.getOutput();
			jobconf.setOutputfolder(outputfolder);
			jobconf.setOutput(null);
			decideContainerCountAndPhysicalMemoryByBlockSize();
			System.setProperty("jobcount", "" + mrtaskcount);
			new File(MDCConstants.LOCAL_FS_APPJRPATH).mkdirs();
			Utils.createJar(new File(MDCConstants.YARNFOLDER), MDCConstants.LOCAL_FS_APPJRPATH, MDCConstants.YARNOUTJAR);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(mapclzchunkfile, yarninputfolder,
					MDCConstants.MASSIVEDATA_YARNINPUT_MAPPER);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(combiner, yarninputfolder,
					MDCConstants.MASSIVEDATA_YARNINPUT_COMBINER);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(reducer, yarninputfolder,
					MDCConstants.MASSIVEDATA_YARNINPUT_REDUCER);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(folderfileblocksmap, yarninputfolder,
					MDCConstants.MASSIVEDATA_YARNINPUT_FILEBLOCKS);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(jobconf, yarninputfolder,
					MDCConstants.MASSIVEDATA_YARNINPUT_CONFIGURATION);
			ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
					MDCConstants.BACKWARD_SLASH + MDCConstants.CONTEXT_FILE_CLIENT, getClass());
			var client = (CommandYarnClient) context.getBean(MDCConstants.YARN_CLIENT);
			client.getEnvironment().put(MDCConstants.YARNMDCJOBID, applicationid);
			var appid = client.submitApplication(true);
			var appreport = client.getApplicationReport(appid);
			while (appreport.getYarnApplicationState() != YarnApplicationState.FINISHED
					&& appreport.getYarnApplicationState() != YarnApplicationState.FAILED) {
				appreport = client.getApplicationReport(appid);
				Thread.sleep(1000);
			}

			log.debug("Waiting for the Reducer to complete------------");
			log.debug("Reducer completed------------------------------");
			jobconf.setOutput(output);
			jm.jobcompletiontime = System.currentTimeMillis();
			jm.totaltimetaken = (jm.jobcompletiontime - jm.jobstarttime) / 1000.0;
			if (!Objects.isNull(jobconf.getOutput())) {
				Utils.writeKryoOutput(kryo, jobconf.getOutput(),
						"Completed Job in " + (jm.totaltimetaken) + " seconds");
			}
			return null;
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.info("Unable To Execute Job, See Cause Below:", ex);
		}
		return null;
	}


	/**
	 * Calculate the container count via number of processors and container
	 * memory
	 * 
	 * @param blocksize
	 * @param stagecount
	 */
	private void decideContainerCountAndPhysicalMemoryByBlockSize() {
		long processors = Integer.parseInt(jobconf.getNumberofcontainers());
		System.setProperty("containercount", "" + (processors - 2));
		System.setProperty("containermemory", "" + Integer.parseInt(jobconf.getMaxmem()));
		System.setProperty("hd.fs", jobconf.getHdfsurl());
		System.setProperty("hd.rm", jobconf.getYarnrm());
		System.setProperty("hd.scheduler", jobconf.getYarnscheduler());
	}

}
