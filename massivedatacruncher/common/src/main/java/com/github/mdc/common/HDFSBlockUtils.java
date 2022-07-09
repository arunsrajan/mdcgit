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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.log4j.Logger;

/**
 * 
 * @author arun
 * The helper or utility class to obtain the blocks information with multiple datanode location.
 */
public class HDFSBlockUtils {

	private HDFSBlockUtils() {
	}
	static Logger log = Logger.getLogger(HDFSBlockUtils.class);

	/**
	 * This function returns list of blocks location using the block size obtained from HDFS.
	 * @param hdfs
	 * @param filepaths
	 * @return list of blocks information with multiple location from HDFS.
	 * @throws Exception
	 */
	public static List<BlocksLocation> getBlocksLocationByFixedBlockSizeAuto(FileSystem hdfs, List<Path> filepaths, boolean isuserdefinedblocksize, long userdefinedblocksize)
			throws Exception {
		var skipbytes = 0l;
		var blsl = new ArrayList<BlocksLocation>();
		var datanodeinfos = new ConcurrentHashMap<String, DatanodeXfefNumAllocated>();
		long startoffset = 0;
		long blocksize;
		for (var filepath : filepaths) {
			try (var hdis = (HdfsDataInputStream) hdfs.open(filepath);) {
				var locatedblocks = hdis.getAllBlocks();
				for (int lbindex = 0; lbindex < locatedblocks.size(); lbindex++) {
					var lb = locatedblocks.get(lbindex);
					blocksize = isuserdefinedblocksize ? lb.getBlockSize() < userdefinedblocksize ? lb.getBlockSize() : userdefinedblocksize : lb.getBlockSize();
					var dinfoa = lb.getLocations();
					var dninfos = Arrays.asList(dinfoa);
					for (var dinfo :dinfoa) {
						var dncna = new DatanodeXfefNumAllocated();
						dncna.ref = dinfo.getXferAddr();
						dncna.numallocated = 0;
						datanodeinfos.put(dncna.ref, dncna);
					}
					while (true) {
						var bls = new BlocksLocation();
						var block = new Block[2];
						block[0] = new Block();
						block[0].blockstart = startoffset;
						startoffset = blocksize + startoffset < lb.getBlockSize() ? startoffset + blocksize : lb.getBlockSize();
						block[0].blockend = startoffset;
						block[0].blockOffset = lb.getStartOffset();
						block[0].filename = filepath.toUri().toString();
						Map<String, Set<String>> dnxref = dninfos.stream().map(dninfo -> dninfo.getXferAddr())
								.collect(Collectors.groupingBy(xrefaddr -> xrefaddr.split(MDCConstants.COLON)[0], Collectors
										.mapping(xrefaddr -> xrefaddr, Collectors.toCollection(LinkedHashSet::new))));
						block[0].dnxref = dnxref;
						bls.block = block;
						blsl.add(bls);
						skipbytes = 0;
						boolean isnewline = isNewLineAtEnd(hdfs, lb, lb.getStartOffset() + startoffset - 1, dninfos.get(0).getXferAddr());
						if (!isnewline) {
							if (startoffset < lb.getBlockSize()) {
								skipbytes = skipBlockToNewLine(hdfs, lb, lb.getStartOffset() + startoffset, dninfos.get(0).getXferAddr());
								if (skipbytes > 0) {
									bls = blsl.get(blsl.size() - 1);
									bls.block[1] = new Block();
									bls.block[1].blockstart = startoffset;
									bls.block[1].blockend = startoffset + skipbytes;
									bls.block[1].blockOffset = lb.getStartOffset();
									bls.block[1].filename = filepath.toUri().toString();
									bls.block[1].dnxref = dninfos.stream().map(dninfo -> dninfo.getXferAddr())
											.collect(Collectors.groupingBy(xrefaddr -> xrefaddr.split(MDCConstants.COLON)[0],
													Collectors.mapping(xrefaddr -> xrefaddr,
															Collectors.toCollection(LinkedHashSet::new))));
									startoffset += skipbytes;
								}
							} else if (lbindex < locatedblocks.size() - 1) {
								lbindex++;
								lb = locatedblocks.get(lbindex);
								startoffset = 0;
								skipbytes = skipBlockToNewLine(hdfs, lb, lb.getStartOffset() + startoffset, dninfos.get(0).getXferAddr());
								if (skipbytes > 0) {
									bls = blsl.get(blsl.size() - 1);
									bls.block[1] = new Block();
									bls.block[1].blockstart = startoffset;
									bls.block[1].blockend = startoffset + skipbytes;
									bls.block[1].blockOffset = lb.getStartOffset();
									bls.block[1].filename = filepath.toUri().toString();
									bls.block[1].dnxref = dninfos.stream().map(dninfo -> dninfo.getXferAddr())
											.collect(Collectors.groupingBy(xrefaddr -> xrefaddr.split(MDCConstants.COLON)[0],
													Collectors.mapping(xrefaddr -> xrefaddr,
															Collectors.toCollection(LinkedHashSet::new))));
									startoffset += skipbytes;
								}
							} else {
								startoffset = 0;
								break;
							}
						} else if (startoffset == lb.getBlockSize()) {
							startoffset = 0;
							break;
						}
						log.debug(blocksize + " " + lb.getStartOffset());
					}
				}
			}
			catch (Exception ex) {
				log.error("Blocks Unavailable due to error", ex);
				throw ex;
			}
		}
		return blsl;
	}

	/**
	 * This function returns the number of bytes to skip to new line from currrent blocks offset. 
	 * @param lblock
	 * @param l
	 * @param xrefaddress
	 * @param hp
	 * @return offset to skip bytes to new line. 
	 * @throws Exception
	 */
	public static long skipToNewLine(LocatedBlock lblock, long l, String xrefaddress, String hp) throws Exception {
		log.debug("Entered HDFSBlockUtils.skipToNewLine");
		var stnl = new SkipToNewLine();
		stnl.l = l;
		stnl.lblock = lblock;
		stnl.xrefaddress = xrefaddress;
		log.debug("In SkipToNewLine client SkipToNewLineObject: " + stnl);

		log.debug("Exiting HDFSBlockUtils.skipToNewLine");
		return (long) Utils.getResultObjectByInput(hp, stnl);
	}

	/**
	 * This function returns the number of bytes to skip to new line from currrent blocks offset given local datanode xref address. 
	 * @param hdfs
	 * @param lblock
	 * @param l
	 * @param xrefaddress
	 * @return offset to skip bytes to new line. 
	 * @throws Exception
	 */
	public static long skipBlockToNewLine(FileSystem hdfs, LocatedBlock lblock, long l, String xrefaddress) throws Exception {
		log.debug("Entered HDFSBlockUtils.skipBlockToNewLine");
		var read1byt = new byte[1];
		var blockReader = HdfsBlockReader.getBlockReader((DistributedFileSystem) hdfs, lblock, l, xrefaddress);
		var skipbytes = 0;
		var builder = new StringBuilder();
		if (blockReader.available() > 0) {
			read1byt[0] = 0;
			while (blockReader.available() > 0) {
				var bytesread = blockReader.read(read1byt, 0, 1);
				builder.append((char) read1byt[0]);
				if (bytesread == 0 || bytesread == -1) {
					break;
				}
				if (read1byt[0] == '\n') {
					skipbytes += 1;
					break;
				}
				skipbytes += 1;
			}
		}
		log.debug("Skip Bytes Read: " + builder.toString());
		log.debug("Exiting HDFSBlockUtils.skipBlockToNewLine");
		return skipbytes;
	}

	public static boolean isNewLineAtEnd(FileSystem hdfs, LocatedBlock lblock, long l, String xrefaddress) throws Exception {
		log.debug("Entered HDFSBlockUtils.skipBlockToNewLine");
		var read1byt = new byte[1];
		var blockReader = HdfsBlockReader.getBlockReader((DistributedFileSystem) hdfs, lblock, l, xrefaddress);
		var builder = new StringBuilder();
		if (blockReader.available() > 0) {
			read1byt[0] = 0;
			while (blockReader.available() > 0) {
				var bytesread = blockReader.read(read1byt, 0, 1);
				builder.append((char) read1byt[0]);
				if (bytesread == 0 || bytesread == -1) {
					break;
				}
				if (read1byt[0] == '\n') {
					return true;
				}
				else {
					return false;
				}
			}
		}
		return false;
	}
}
