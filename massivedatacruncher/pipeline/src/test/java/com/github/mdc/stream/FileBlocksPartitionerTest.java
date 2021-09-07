package com.github.mdc.stream;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.Job;
import com.github.mdc.common.JobMetrics;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.MDCProperties;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.MassiveDataPipelineException;
import com.github.mdc.stream.MassiveDataPipelineIgnite;

public class FileBlocksPartitionerTest extends MDCPipelineTestsCommon{
	static Ignite server;
	@SuppressWarnings("rawtypes")
	@BeforeClass
	public static void launchNodes() throws Exception {
		Utils.loadLog4JSystemPropertiesClassPath("mdctest.properties");
		Ignition.setClientMode(false);
		var cfg = new IgniteConfiguration();
		cfg.setIgniteInstanceName("Server");
		// The node will be started as a server node.
		cfg.setClientMode(false);
		cfg.setDeploymentMode(DeploymentMode.CONTINUOUS);
		// Classes of custom Java logic will be transferred over the wire from
		// this app.
		cfg.setPeerClassLoadingEnabled(true);
		// Setting up an IP Finder to ensure the client can locate the servers.
		var ipFinder = new TcpDiscoveryMulticastIpFinder();
		ipFinder.setMulticastGroup(MDCProperties.get().getProperty(MDCConstants.IGNITEMULTICASTGROUP));
		cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));
		var cc = new CacheConfiguration(MDCConstants.MDCCACHE);
		cc.setCacheMode(CacheMode.PARTITIONED);
		cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
		cc.setBackups(Integer.parseInt(MDCProperties.get().getProperty(MDCConstants.IGNITEBACKUP)));
		cfg.setCacheConfiguration(cc);
		// Starting the node
		server = Ignition.start(cfg);
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testgetJobStageBlocks() throws MassiveDataPipelineException {
		Job job = new Job();
		job.jm = new JobMetrics();
		PipelineConfig pc = new PipelineConfig();
		MassiveDataPipelineIgnite mdpi = MassiveDataPipelineIgnite.newStreamFILE(System.getProperty("user.dir")+MDCConstants.BACKWARD_SLASH+"src/test/resources/ignite", pc).map(val->val.split(MDCConstants.COMMA));
		((MassiveDataPipelineIgnite)mdpi.root).mdsroots.add(mdpi.root);
		((MassiveDataPipelineIgnite)mdpi.root).finaltasks = new HashSet<>(Arrays.asList(mdpi.root.finaltask));
		((MassiveDataPipelineIgnite)mdpi.root).getDAG(job);
		List<BlocksLocation> bls = (List<BlocksLocation>) job.stageoutputmap.get(job.stageoutputmap.keySet().iterator().next());
		assertEquals(1,bls.size());
		assertEquals(2,bls.get(0).block.length);
		assertEquals(4270834,bls.get(0).block[0].blockend);
		job.igcache.close();
		job.ignite.close();
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testgetJobStageBlocks32MBBlockSize() throws MassiveDataPipelineException {
		Job job = new Job();
		job.jm = new JobMetrics();
		PipelineConfig pc = new PipelineConfig();
		pc.setBlocksize("32");
		MassiveDataPipelineIgnite mdpi = MassiveDataPipelineIgnite.newStreamFILE("E:\\DEVELOPMENT\\dataset\\airline\\1987", pc).map(val->val.split(MDCConstants.COMMA));
		((MassiveDataPipelineIgnite)mdpi.root).mdsroots.add(mdpi.root);
		((MassiveDataPipelineIgnite)mdpi.root).finaltasks = new HashSet<>(Arrays.asList(mdpi.root.finaltask));
		((MassiveDataPipelineIgnite)mdpi.root).getDAG(job);
		List<BlocksLocation> bls = (List<BlocksLocation>) job.stageoutputmap.get(job.stageoutputmap.keySet().iterator().next());
		assertEquals(4,bls.size());
		var sum = 0;
		for(int index=0;index<bls.size();index++) {
			BlocksLocation bl = bls.get(index);
			sum += bl.block[0].blockend - bl.block[0].blockstart; 
		}
		assertEquals(127162942,sum);
		job.igcache.close();
		job.ignite.close();
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testgetJobStageBlocks64MBBlockSize() throws MassiveDataPipelineException {
		Job job = new Job();
		job.jm = new JobMetrics();
		PipelineConfig pc = new PipelineConfig();
		pc.setBlocksize("64");
		MassiveDataPipelineIgnite mdpi = MassiveDataPipelineIgnite.newStreamFILE("E:\\DEVELOPMENT\\dataset\\airline\\1989", pc).map(val->val.split(MDCConstants.COMMA));
		((MassiveDataPipelineIgnite)mdpi.root).mdsroots.add(mdpi.root);
		((MassiveDataPipelineIgnite)mdpi.root).finaltasks = new HashSet<>(Arrays.asList(mdpi.root.finaltask));
		((MassiveDataPipelineIgnite)mdpi.root).getDAG(job);
		List<BlocksLocation> bls = (List<BlocksLocation>) job.stageoutputmap.get(job.stageoutputmap.keySet().iterator().next());
		assertEquals(8,bls.size());
		var sum = 0;
		for(int index=0;index<bls.size();index++) {
			BlocksLocation bl = bls.get(index);
			sum += bl.block[0].blockend - bl.block[0].blockstart; 
		}
		assertEquals(486518821,sum);
		job.igcache.close();
		job.ignite.close();
	}
	@AfterClass
	public static void shutdownNodes() throws Exception {
		if(!Objects.isNull(server)) {
			server.close();
		}
	}
	
}
