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
package com.github.mdc.tasks.scheduler;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
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

import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.HDFSBlockUtils;
import com.github.mdc.common.HeartBeat;
import com.github.mdc.common.JobConfiguration;
import com.github.mdc.common.JobMetrics;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCJobMetrics;
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
	List<LocatedBlock> locatedBlocks;
	int executorindex;
	ExecutorService es;
	HeartBeat hbs;

	public MapReduceApplicationYarn(String jobname, JobConfiguration jobconf, List<MapperInput> mappers,
			List<Class<?>> combiners, List<Class<?>> reducers, String outputfolder) {
		this.jobname = jobname;
		this.jobconf = jobconf;
		this.mappers = mappers;
		this.combiners = combiners;
		this.reducers = reducers;
		this.outputfolder = outputfolder;
	}


	Map<String, ArrayBlockingQueue> containerqueue = new ConcurrentHashMap<>();
	List<Integer> ports;
	protected List<String> containers;
	protected List<String> nodessorted;

	public void getDnXref(List<BlocksLocation> bls) throws MapReduceException {
		log.debug("Entered MdcJob.getDnXref");
		var dnxrefs = bls.stream().parallel().flatMap(bl -> {
			var xrefs = new LinkedHashSet<String>();
			Iterator<Set<String>> xref = bl.getBlock()[0].getDnxref().values().iterator();
			for (; xref.hasNext(); ) {
				xrefs.addAll(xref.next());
			}
			if (bl.getBlock().length > 1 && !Objects.isNull(bl.getBlock()[1])) {
				xref = bl.getBlock()[0].getDnxref().values().iterator();
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
			var xrefselected = b.getBlock()[0].getDnxref().keySet().stream()
					.flatMap(xrefhost -> b.getBlock()[0].getDnxref().get(xrefhost).stream()).sorted((xref1, xref2) -> {
				return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
			}).findFirst();
			var xref = xrefselected.get();
			dnxrefallocatecount.put(xref, dnxrefallocatecount.get(xref) + 1);
			b.getBlock()[0].setHp(xref);
			if (b.getBlock().length > 1 && !Objects.isNull(b.getBlock()[1])) {
				xrefselected = b.getBlock()[1].getDnxref().keySet().stream()
						.flatMap(xrefhost -> b.getBlock()[1].getDnxref().get(xrefhost).stream()).sorted((xref1, xref2) -> {
					return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
				}).findFirst();
				xref = xrefselected.get();
				dnxrefallocatecount.put(xref, dnxrefallocatecount.get(xref) + 1);
				b.getBlock()[1].setHp(xref);
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
			jm.setJobstarttime(System.currentTimeMillis());
			jm.setJobid(applicationid);
			MDCJobMetrics.put(jm);
			hdfs = FileSystem.get(new URI(MDCProperties.get().getProperty(MDCConstants.HDFSNAMENODEURL)),
					configuration);
			
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
				jm.setTotalfilesize(jm.getTotalfilesize() + Utils.getTotalLengthByFiles(hdfs, blockpath));
				blockpath.clear();
			}

			jm.setTotalfilesize(jm.getTotalfilesize() / MDCConstants.MB);
			jm.setFiles(allfiles);
			jm.setMode(jobconf.getExecmode());
			jm.setTotalblocks(bls.size());
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
			var yarninputfolder = MDCConstants.YARNINPUTFOLDER + MDCConstants.FORWARD_SLASH + applicationid;
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
					MDCConstants.FORWARD_SLASH + MDCConstants.CONTEXT_FILE_CLIENT, getClass());
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
			jm.setJobcompletiontime(System.currentTimeMillis());
			jm.setTotaltimetaken((jm.getJobcompletiontime() - jm.getJobstarttime()) / 1000.0);
			if (!Objects.isNull(jobconf.getOutput())) {
				Utils.writeToOstream(jobconf.getOutput(),
						"Completed Job in " + (jm.getTotaltimetaken()) + " seconds");
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
