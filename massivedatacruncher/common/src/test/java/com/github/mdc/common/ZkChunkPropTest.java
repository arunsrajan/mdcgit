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

import org.junit.Test;

import junit.framework.TestCase;

public class ZkChunkPropTest extends TestCase {
	public static final Integer numberofshards = 10;
	public static final Integer numberofreplicas = 4;
	public static final String files = "chunk.csv";
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
