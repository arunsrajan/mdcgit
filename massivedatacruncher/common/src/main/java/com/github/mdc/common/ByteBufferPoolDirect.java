package com.github.mdc.common;

import java.util.Objects;

import com.github.pbbl.direct.DirectByteBufferPool;
import com.github.pbbl.heap.ByteBufferPool;

public class ByteBufferPoolDirect {
	private static ByteBufferPool pool = null;
	public static void init(int maxpoolsize) {
		if(Objects.isNull(pool)) {
			ByteBufferPoolDirect.pool=new ByteBufferPool();
		}
	}
	public static ByteBufferPool get() {
		return ByteBufferPoolDirect.pool;
	}
}
