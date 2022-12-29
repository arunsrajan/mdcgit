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
import org.xerial.snappy.SnappyOutputStream;
import com.github.mdc.common.Block;
import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.Job;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCIgniteClient;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Stage;
import com.github.mdc.stream.AbstractPipeline;
import com.github.mdc.stream.IgnitePipeline;
import com.github.mdc.stream.PipelineException;

public class FileBlocksPartitioner {

	PipelineConfig pc;
	Job job;

	@SuppressWarnings("rawtypes")
	public void getJobStageBlocks(Job job, PipelineConfig pipelineconfig, String folder, Collection<AbstractPipeline> mdsroots, Set<Stage> rootstages) throws PipelineException {
		pc = pipelineconfig;
		this.job = job;
		var roots = mdsroots.iterator();
		// Getting the ignite client
		var ignite = MDCIgniteClient.instance(pc);
		IgniteCache<Object, byte[]> ignitecache = ignite.cache(MDCConstants.MDCCACHE);
		job.setIgnite(ignite);
		var computeservers = job.getIgnite().cluster().forServers();
		job.getJm().setContainersallocated(computeservers.hostNames().stream().collect(Collectors.toMap(key -> key, value -> 0d)));
		job.setIgcache(ignitecache);
		job.setStageoutputmap(new ConcurrentHashMap<>());
		for (var rootstage : rootstages) {
			var obj = roots.next();
			if (obj instanceof IgnitePipeline mdp) {
				folder = mdp.getFolder();
			}

			var files = new File(folder).listFiles();
			var totalsplits = 0;
			var bls = new ArrayList<BlocksLocation>();
			for (var csvfile : files) {
				if (csvfile.isFile()) {
					partitionFiles(ignitecache, csvfile.getAbsolutePath(), bls);
				}
			}
			job.getStageoutputmap().put(rootstage, bls);
		}
	}

	protected void partitionFiles(IgniteCache<Object, byte[]> cache, String filepath,
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
					bl.getBlock()[0] = new Block();
					bl.getBlock()[0].setBlockstart(totalbytes);
					readWrite(raf, lzfos, bytesPerSplit);
					skip = addBytesToNewline(raf, lzfos);
					totalskip += skip;
					totalbytes += bytesPerSplit + skip;
					lzfos.flush();
					bl.getBlock()[0].setBlockend(totalbytes);
					bl.getBlock()[0].setFilename(filepath);
					cache.putIfAbsent(bl, baos.toByteArray());
					bls.add(bl);
				}
			}
			remaining -= totalskip;
			if (remaining > 0) {
				numSplits++;
				try (var baos = new ByteArrayOutputStream(); var lzfos = new SnappyOutputStream(baos);) {
					var bl = new BlocksLocation();
					bl.getBlock()[0] = new Block();
					bl.getBlock()[0].setBlockstart(totalbytes);
					readWrite(raf, lzfos, remaining);
					totalbytes += remaining;
					bl.getBlock()[0].setBlockend(totalbytes);
					bl.getBlock()[0].setFilename(filepath);
					cache.putIfAbsent(bl, baos.toByteArray());
					bls.add(bl);
				}
			}
		}
		catch (Exception e) {
			throw new PipelineException(MDCConstants.FILEBLOCKSPARTITIONINGERROR, e);
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
			if (numread == -1 || numread == 0) {
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
