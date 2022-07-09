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

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.ehcache.Cache;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xerial.snappy.SnappyInputStream;

public class CacheUtilsTest {

	String hdfsurl = "hdfs://127.0.0.1:9000";
	String[] hdfsdirpaths = {"/airlines"};

	@BeforeClass
	public static void initCache() throws Exception {
		Utils.loadLog4JSystemPropertiesClassPath(MDCConstants.MDC_TEST_PROPERTIES);
		CacheUtils.initCache();
		ByteBufferPoolDirect.init();
		ByteBufferPool.init(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.BYTEBUFFERPOOL_MAX, MDCConstants.BYTEBUFFERPOOL_MAX_DEFAULT)));
	}

	@Test
	public void testCache() throws Exception {
		FileSystem hdfs = FileSystem.get(new URI(hdfsurl), new Configuration());
		List<Path> blockpath  = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths) {
			FileStatus[] fileStatus = hdfs.listStatus(
					new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			blockpath.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, blockpath, true, 128 * MDCConstants.MB);
		getDnXref(bls);
		String cacheblock = "cacheblock";
		int blscount = 0;
		Cache<String, byte[]> cache = (Cache<String, byte[]>) MDCCache.get();
		for (BlocksLocation bl :bls) {
			SnappyInputStream sis = HdfsBlockReader.getBlockDataSnappyStream(bl, hdfs);
			byte[] byt = sis.readAllBytes();
			cache.put(cacheblock + blscount, byt);
			blscount++;
			System.out.println(blscount);
			sis.close();
		}
	}

	public void getDnXref(List<BlocksLocation> bls) {

		var dnxrefs = bls.stream().parallel().flatMap(bl -> {
			var xrefs = new LinkedHashSet<String>();
			Iterator<Set<String>> xref = bl.block[0].dnxref.values().iterator();
			for (; xref.hasNext(); ) {
				xrefs.addAll(xref.next());
			}
			if(bl.block.length > 1 && !Objects.isNull(bl.block[1])) {
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
			if(b.block.length > 1 && !Objects.isNull(b.block[1])) {
				xrefselected = b.block[1].dnxref.keySet().stream()
						.flatMap(xrefhost -> b.block[1].dnxref.get(xrefhost).stream()).sorted((xref1, xref2) -> {
					return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
				}).findFirst();
				xref = xrefselected.get();
				b.block[1].hp = xref;
			}
		}
	}

	@AfterClass
	public static void destroyCache() throws Exception {
		MDCCache.get().clear();
		MDCCacheManager.get().close();
		ByteBufferPoolDirect.get().close();
	}

}
