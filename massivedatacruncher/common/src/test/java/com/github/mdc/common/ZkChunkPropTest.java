package com.github.mdc.common;

import org.junit.Test;

import com.github.mdc.common.ZkChunkProp;

import junit.framework.TestCase;

public class ZkChunkPropTest extends TestCase{
	public static final Integer numberofshards = 10;
	public static final Integer numberofreplicas =4;
	public static final String files ="chunk.csv";
	public static final String hostsinvolved = "127.0.0.1:8080";
	public static final Boolean isdatasparsed = true;
	
	@Test
	public void testNumberofShards() {
		ZkChunkProp zcp = new ZkChunkProp();
		zcp.setNumberofshards(numberofshards);
		assertEquals(numberofshards, zcp.getNumberofshards());
	}
	
	@Test
	public void testNumberofShardsNull() {
		ZkChunkProp zcp = new ZkChunkProp();
		zcp.setNumberofshards(null);
		assertEquals(null, zcp.getNumberofshards());
	}
	
	@Test
	public void testNumberofreplicas() {
		ZkChunkProp zcp = new ZkChunkProp();
		zcp.setNumberofreplicas(numberofreplicas);
		assertEquals(numberofreplicas, zcp.getNumberofreplicas());
	}
	
	@Test
	public void testNumberofreplicasNull() {
		ZkChunkProp zcp = new ZkChunkProp();
		zcp.setNumberofreplicas(null);
		assertEquals(null, zcp.getNumberofreplicas());
	}
	
	
	@Test
	public void testFiles() {
		ZkChunkProp zcp = new ZkChunkProp();
		zcp.setFiles(files);
		assertEquals(files, zcp.getFiles());
	}
	
	@Test
	public void testFilesNull() {
		ZkChunkProp zcp = new ZkChunkProp();
		zcp.setFiles(null);
		assertEquals(null, zcp.getFiles());
	}
	
	@Test
	public void testHostsinvolved() {
		ZkChunkProp zcp = new ZkChunkProp();
		zcp.setHostsinvolved(hostsinvolved);
		assertEquals(hostsinvolved, zcp.getHostsinvolved());
	}
	
	@Test
	public void testIsdatasparsed() {
		ZkChunkProp zcp = new ZkChunkProp();
		zcp.setIsdatasparsed(isdatasparsed);
		assertEquals(isdatasparsed, zcp.getIsdatasparsed());
	}
	@Test
	public void testIsdatasparsedNull() {
		ZkChunkProp zcp = new ZkChunkProp();
		zcp.setIsdatasparsed(null);
		assertEquals(null, zcp.getIsdatasparsed());
	}
	
	
}
