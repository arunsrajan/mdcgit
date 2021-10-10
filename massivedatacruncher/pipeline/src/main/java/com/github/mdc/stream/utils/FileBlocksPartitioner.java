package com.github.mdc.stream.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.xerial.snappy.SnappyOutputStream;

import com.github.mdc.common.Block;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.Job;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Stage;
import com.github.mdc.stream.AbstractPipeline;
import com.github.mdc.stream.PipelineException;
import com.github.mdc.stream.IgnitePipeline;

public class FileBlocksPartitioner {
	
	PipelineConfig pc;  
	Job job;
	@SuppressWarnings("rawtypes")
	public void getJobStageBlocks(Job job, PipelineConfig pipelineconfig, String folder,Collection<AbstractPipeline> mdsroots, Set<Stage> rootstages) throws PipelineException {
		pc = pipelineconfig;
		this.job = job;
		var roots = mdsroots.iterator();
		var cfg = new IgniteConfiguration();
		// The node will be started as a client node.
		cfg.setClientMode(true);
		cfg.setDeploymentMode(DeploymentMode.CONTINUOUS);
		// Classes of custom Java logic will be transferred over the wire from
		// this app.
		cfg.setPeerClassLoadingEnabled(true);
		// Setting up an IP Finder to ensure the client can locate the servers.
		var ipFinder = new TcpDiscoveryMulticastIpFinder();
		ipFinder.setMulticastGroup(pipelineconfig.getIgnitemulticastgroup());
		cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));
		var cc = new CacheConfiguration(MDCConstants.MDCCACHE);
		cc.setCacheMode(CacheMode.PARTITIONED);
		cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		cc.setBackups(Integer.parseInt(pipelineconfig.getIgnitebackup()));
		cfg.setCacheConfiguration(cc);
		// Starting the node
		var ignite = Ignition.start(cfg);
		IgniteCache<Object, byte[]> ignitecache = ignite.cache(MDCConstants.MDCCACHE);
		job.ignite = ignite;
		var computeservers = job.ignite.cluster().forServers();
		job.jm.containersallocated = computeservers.hostNames().stream().collect(Collectors.toMap(key->key, value->0d));
		job.igcache = ignitecache;
		job.stageoutputmap = new ConcurrentHashMap<>();
		for (var rootstage : rootstages) {
			var obj = roots.next();
			if (obj instanceof IgnitePipeline mdp) {
				folder = mdp.getFolder();
			}

			var files = new File(folder).listFiles();
			var totalsplits = 0;
			var bls = new ArrayList<BlocksLocation>();
			for (var csvfile : files) {
				if(csvfile.isFile()) {
					partitionFiles(ignitecache, csvfile.getAbsolutePath(), totalsplits, bls);
				}
			}
			job.stageoutputmap.put(rootstage, bls);
		}
	}
	
	protected void partitionFiles(IgniteCache<Object, byte[]> cache, String filepath, int id,
			List<BlocksLocation> bls) throws PipelineException {
		try (var raf = new RandomAccessFile(filepath, "r");) {
			var sourceSize = raf.length();
			var fileblocksizemb = Integer.parseInt(pc.getBlocksize());
			var bytesPerSplit = fileblocksizemb * 1024 * 1024;
			bytesPerSplit = (int) (sourceSize < bytesPerSplit ? sourceSize : bytesPerSplit);
			var numSplits = sourceSize / bytesPerSplit;
			var remaining = sourceSize % bytesPerSplit;
			var totalbytes = 0;
			int destIx;
			var skip = 0;
			var totalskip = 0;
			for (destIx = 1; destIx <= numSplits; destIx++) {
				try (var baos = new ByteArrayOutputStream(); var lzfos = new SnappyOutputStream(baos);) {
					var bl = new BlocksLocation();
					bl.block[0] = new Block();
					bl.block[0].blockstart = totalbytes;
					readWrite(raf, lzfos, bytesPerSplit);
					skip = addBytesToNewline(raf, lzfos);
					totalskip += skip;
					totalbytes += bytesPerSplit + skip;
					lzfos.flush();
					bl.block[0].blockend = totalbytes;
					bl.block[0].filename = filepath;
					cache.putIfAbsent(bl, baos.toByteArray());
					bls.add(bl);
				}
			}
			remaining -= totalskip;
			if (remaining > 0) {
				numSplits++;
				try (var baos = new ByteArrayOutputStream(); var lzfos = new SnappyOutputStream(baos);) {
					var bl = new BlocksLocation();
					bl.block[0] = new Block();
					bl.block[0].blockstart = totalbytes;
					readWrite(raf, lzfos, remaining);
					totalbytes += remaining;
					bl.block[0].blockend = totalbytes;
					bl.block[0].filename = filepath;
					cache.putIfAbsent(bl, baos.toByteArray());
					bls.add(bl);
				}
			}
		}
		catch(Exception e) {
			throw new PipelineException(MDCConstants.FILEBLOCKSPARTITIONINGERROR,e);
		}
	}

	protected void readWrite(RandomAccessFile raf, OutputStream os, long numBytes) throws IOException {
		var buf = new byte[(int) numBytes];
		var val = raf.read(buf);
		if (val != -1) {
			os.write(buf);
		}
		os.flush();
	}

	protected int addBytesToNewline(RandomAccessFile raf, OutputStream os) throws IOException {
		var ch = new byte[1];
		var skip = 0;
		while (true) {
			int numread = raf.read(ch);
			if(numread==-1||numread==0) {
				break;
			}
			skip++;
			if (ch[0] == '\n') {
				break;
			}
			os.write(ch[0]);
		}
		os.flush();
		return skip;
	}
}
