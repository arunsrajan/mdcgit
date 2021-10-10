package com.github.mdc.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.ref.SoftReference;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.BlockReader;
import org.apache.hadoop.hdfs.ClientContext;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.RemotePeerFactory;
import org.apache.hadoop.hdfs.client.HdfsDataInputStream;
import org.apache.hadoop.hdfs.client.impl.BlockReaderFactory;
import org.apache.hadoop.hdfs.client.impl.DfsClientConf;
import org.apache.hadoop.hdfs.net.NioInetPeer;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.htrace.core.Tracer;
import org.apache.log4j.Logger;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;


/**
 * 
 * @author 
 * Arun HDFS Block Reader for data locality
 */
public class HdfsBlockReader {

	private static Logger log = Logger.getLogger(HdfsBlockReader.class);

	/**
	 * This method gets the data in bytes from hdfs given the blocks location.
	 * @param bl
	 * @param hdfs
	 * @return byte array 
	 * @throws Exception
	 */
	public static byte[] getBlockDataMR(final BlocksLocation bl, FileSystem hdfs) throws Exception {
		try {
			log.debug("Entered HdfsBlockReader.getBlockDataMR");
			var baos = new ByteArrayOutputStream();
			var mapfilenamelb = new HashMap<String, List<LocatedBlock>>();
			for (var block : bl.block) {
				log.debug("In getBlockDataMR block: " + block);
				if (!Objects.isNull(block) && Objects.isNull(mapfilenamelb.get(block.filename))) {
					try (var fsinput = (HdfsDataInputStream) hdfs.open(new Path(block.filename));) {
						mapfilenamelb.put(block.filename, new ArrayList<>(fsinput.getAllBlocks()));
					}
				}
				if (!Objects.isNull(block)) {
					var locatedBlocks = mapfilenamelb.get(block.filename);
					for (var lb : locatedBlocks) {
						if (lb.getStartOffset() == block.blockOffset) {
							log.debug("Obtaining Data for the " + block + " with offset: " + lb.getStartOffset());
							getDataBlock(block, lb, hdfs, baos, block.hp.split(MDCConstants.UNDERSCORE)[0]);
							break;
						}
					}
				}
			}
			// Data bytes for processing
			baos.flush();
			var byt = baos.toByteArray();
			var srbaos = new SoftReference<ByteArrayOutputStream>(baos);
			srbaos.clear();
			baos = null;
			log.debug("Exiting HdfsBlockReader.getBlockDataMR");
			return byt;
		} catch (Exception ex) {
			log.error("Unable to Obtain Block Data getBlockDataMR: ", ex);
		}

		return null;

	}

	/**
	 * This method gets the data in bytes.
	 * @param block
	 * @param lb
	 * @param hdfs
	 * @param baos
	 * @param containerhost
	 * @return byte array
	 */
	public static byte[] getDataBlock(Block block, LocatedBlock lb, FileSystem hdfs, OutputStream baos,
			String containerhost) {
		try {
			log.debug("Entered HdfsBlockReader.getDataBlock");
			// Block reader to get file block bytes
			var totalbytestoread = (int) (block.blockend - block.blockstart);
			var breader = getBlockReader((DistributedFileSystem) hdfs, lb, lb.getStartOffset()+block.blockstart, containerhost);
			log.debug("In getDataBlock Read Bytes: " + totalbytestoread);
			var readsize = 1024;
			var byt = new byte[readsize];
			
			var sum = 0;
			// Number of bytes to read.
			if (breader.available() > 0) {
				log.debug("In getDataBlock BlockReader: " + breader.getNetworkDistance());
				while (breader.available() > 0) {
					var read = breader.read(byt, 0, readsize);
					if (read == -1)
						break;
					// If bytes chunk read are less than or equal to
					// total number of bytes to read for processing.
					if (sum + read <= totalbytestoread) {
						baos.write(byt, 0, read);
						baos.flush();
					}
					// If total chunk bytes read exceeds total bytes to
					// process, write to stream only required bytes.
					else {
						log.debug("In getDataBlock while break: " + (sum + (totalbytestoread - sum)));
						baos.write(byt, 0, (totalbytestoread - sum));
						baos.flush();
						break;
					}
					sum += read;
				}
			}
			breader.close();
			log.debug("Exiting HdfsBlockReader.getDataBlock");
		} catch (Exception ex) {
			log.error("Unable to Obtain Block Data: ", ex);
		}

		return null;
	}
	
	
	/**
	 * 
	 * This function returns compressed data stream using LZF compression.
	 * @param bl
	 * @param hdfs
	 * @return
	 * @throws Exception
	 */
	public static SnappyInputStream getBlockDataLZFStream(final BlocksLocation bl, FileSystem hdfs) throws Exception {
		ByteBuffer bb = ByteBufferPool.get().borrowObject();
		try(var bbos = new ByteBufferOutputStream(bb);
				var lzfos = new SnappyOutputStream(bbos)) {
			log.debug("Entered HdfsBlockReader.getBlockDataMR");
			
			
			var mapfilenamelb = new HashMap<String, List<LocatedBlock>>();
			for (var block : bl.block) {
				log.debug("In getBlockDataMR block: " + block);
				if (!Objects.isNull(block) && Objects.isNull(mapfilenamelb.get(block.filename))) {
					try (var fsinput = (HdfsDataInputStream) hdfs.open(new Path(block.filename));) {
						mapfilenamelb.put(block.filename, new ArrayList<>(fsinput.getAllBlocks()));
					}
				}
				if (!Objects.isNull(block)) {
					var locatedBlocks = mapfilenamelb.get(block.filename);
					for (var lb : locatedBlocks) {
						if (lb.getStartOffset() == block.blockOffset) {
							log.debug("Obtaining Data for the " + block + " with offset: " + lb.getStartOffset());
							getDataBlock(block, lb, hdfs, lzfos, block.hp.split(MDCConstants.UNDERSCORE)[0]);
							break;
						}
					}
				}
			}
			bbos.get().flip();
			log.debug("Exiting HdfsBlockReader.getBlockDataMR");
			return new SnappyInputStream(new ByteBufferInputStream(bbos.get()));
		} catch (Exception ex) {
			log.error("Unable to Obtain Block Data getBlockDataMR: ", ex);
		}

		return null;

	}
	
	public static Set<BlockExecutors> sort(Set<BlockExecutors> blocks) {
		return blocks.parallelStream().sorted((b1, b2) -> {
			return b1.numberofblockstoread - b2.numberofblockstoread;
		}).collect(Collectors.toCollection(LinkedHashSet::new));
	}

	public static String getLocation(Collection<String> locationsblock, Set<BlockExecutors> blocks) {
		for (var be : blocks) {
			if (locationsblock.contains(be.hp)) {
				be.numberofblockstoread++;
				return be.hp;
			}
		}
		return null;
	}

	/**
	 * The block reader for reading block information.
	 * @param fs
	 * @param lb
	 * @param offset
	 * @param xrefaddress
	 * @return block reader object.
	 * @throws IOException
	 */
	public static BlockReader getBlockReader(final DistributedFileSystem fs, LocatedBlock lb, long offset,
			String xrefaddress) throws IOException {
		log.debug("Entered HdfsBlockReader.getBlockReader");
		InetSocketAddress targetAddr = null;
		var eblock = lb.getBlock();
		var nodes = lb.getLocations();
		var dninfos = Arrays.asList(nodes);
		var dnaddress = dninfos.stream().filter(dninfo -> dninfo.getXferAddr().contains(xrefaddress))
				.findFirst();
		DatanodeInfo dninfo;
		if (dnaddress.isEmpty()) {
			targetAddr = NetUtils.createSocketAddr(nodes[0].getXferAddr());
			dninfo = nodes[0];
		} else {
			targetAddr = NetUtils.createSocketAddr(dnaddress.get().getXferAddr());
			dninfo = dnaddress.get();
		}
		var offsetIntoBlock = offset - lb.getStartOffset();
		log.debug("Extended Block Num Bytes: "+eblock.getNumBytes());
		log.debug("Offset Within Block: "+offsetIntoBlock);
		log.debug("Xref Address Address: " + dninfo);
		log.debug("Target Address: " + targetAddr);
		fs.getConf().setBoolean("dfs.client.read.shortcircuit", true);
		fs.getConf().setBoolean("dfs.client.use.legacy.blockreader.local", true);
		var dfsClientConf = new DfsClientConf(fs.getConf());
		var clientContext = ClientContext.get("MDCContext", dfsClientConf, fs.getConf());
		log.debug("Use Legacy Block Reader Local: " + clientContext.getUseLegacyBlockReaderLocal());
		log.debug("Using Legacy Block Reader: " + dfsClientConf.getShortCircuitConf().isUseLegacyBlockReaderLocal());
		log.debug("Using Legacy Block Reader Local: "
				+ dfsClientConf.getShortCircuitConf().isUseLegacyBlockReaderLocal());
		log.debug("Exiting HdfsBlockReader.getBlockReader");
		return new BlockReaderFactory(dfsClientConf).setInetSocketAddress(targetAddr).setBlock(eblock)
				.setFileName(targetAddr.toString() + MDCConstants.COLON + eblock.getBlockId())
				.setBlockToken(lb.getBlockToken()).setStartOffset(offsetIntoBlock).setLength(lb.getBlockSize()-offsetIntoBlock)
				.setVerifyChecksum(true).setClientName(MDCConstants.MDC).setDatanodeInfo(dninfo)
				.setClientCacheContext(clientContext).setCachingStrategy(CachingStrategy.newDefaultStrategy())
				.setConfiguration(fs.getConf()).setTracer(new Tracer.Builder("MDCTracer").build())
				.setStorageType(StorageType.DISK).setAllowShortCircuitLocalReads(true)
				.setRemotePeerFactory(new RemotePeerFactory() {

					public Peer newConnectedPeer(InetSocketAddress addr, Token<BlockTokenIdentifier> blockToken,
							DatanodeID datanodeId) throws IOException {
						Peer peer = null;
						var sock = NetUtils.getDefaultSocketFactory(fs.getConf()).createSocket();
						try {
							sock.connect(addr, 10000);
							sock.setSoTimeout(10000);
							peer = new NioInetPeer(sock);
						} finally {
							if (peer == null) {
								IOUtils.closeQuietly(sock);
							}
						}
						return peer;
					}
				}).build();
	}
}
