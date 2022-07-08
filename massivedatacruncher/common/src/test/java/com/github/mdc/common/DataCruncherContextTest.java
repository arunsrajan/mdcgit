package com.github.mdc.common;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.github.mdc.common.DataCruncherContext;

public class DataCruncherContextTest {
	String testdata = "TestData";
	String testdata1 = "TestData1";
	String testdata2 = "TestData2";
	@Test
	public void testPutGet() {
		DataCruncherContext<String, String> ctx = new DataCruncherContext<>();
		ctx.put(testdata, testdata);
		ctx.put(testdata, testdata1);
		ctx.put(testdata, testdata2);
		
		assertTrue(ctx.get(testdata).contains(testdata));
		assertTrue(ctx.get(testdata).contains(testdata1));
		assertTrue(ctx.get(testdata).contains(testdata2));
	}
	@Test
	public void testKeys() {
		DataCruncherContext<String, String> ctx = new DataCruncherContext<>();
		ctx.put(testdata, testdata);
		ctx.put(testdata, testdata1);
		ctx.put(testdata, testdata2);
		
		assertTrue(ctx.keys().size() == 1);
		assertTrue(ctx.keys().contains(testdata));
		assertFalse(ctx.keys().contains(testdata1));
	}
	
	@Test
	public void testAddAll() {
		DataCruncherContext<String, String> ctx = new DataCruncherContext<>();
		List<String> coll = Arrays.asList(testdata, testdata1, testdata2);
		ctx.addAll(testdata, coll);
		assertTrue(ctx.keys().size() == 1);
		assertTrue(ctx.keys().contains(testdata));
		assertFalse(ctx.keys().contains(testdata1));
		assertTrue(ctx.get(testdata).contains(testdata));
		assertTrue(ctx.get(testdata).contains(testdata1));
		assertTrue(ctx.get(testdata).contains(testdata2));
	}
	
	@Test
	public void testPutAll() {
		DataCruncherContext<String, String> ctx = new DataCruncherContext<>();
		Set<String> coll = new LinkedHashSet<>(Arrays.asList(testdata, testdata1, testdata2));
		ctx.putAll(coll, testdata);
		assertTrue(ctx.keys().size() == 3);
		assertTrue(ctx.keys().contains(testdata));
		assertTrue(ctx.keys().contains(testdata1));
		assertTrue(ctx.keys().contains(testdata2));
		assertTrue(ctx.get(testdata1).contains(testdata));
		assertTrue(ctx.get(testdata2).contains(testdata));
		assertTrue(ctx.get(testdata).contains(testdata));
	}
	@Test
	public void testAdd() {
		DataCruncherContext<String, String> ctx = new DataCruncherContext<>();
		Set<String> coll = new LinkedHashSet<>(Arrays.asList(testdata, testdata1, testdata2));
		ctx.putAll(coll, testdata);
		DataCruncherContext<String, String> ctxtest = new DataCruncherContext<>();
		ctxtest.add(ctx);
		assertTrue(ctxtest.keys().size() == 3);
		assertTrue(ctxtest.keys().contains(testdata));
		assertTrue(ctxtest.keys().contains(testdata1));
		assertTrue(ctxtest.keys().contains(testdata2));
		assertTrue(ctxtest.get(testdata1).contains(testdata));
		assertTrue(ctxtest.get(testdata2).contains(testdata));
		assertTrue(ctxtest.get(testdata).contains(testdata));
	}
}
