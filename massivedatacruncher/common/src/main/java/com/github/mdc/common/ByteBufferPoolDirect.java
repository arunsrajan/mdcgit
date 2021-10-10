package com.github.mdc.common;

import java.util.Objects;

import com.github.pbbl.direct.DirectByteBufferPool;

public class ByteBufferPoolDirect {
	private static DirectByteBufferPool pool = null;
	public static void init() {
		if(Objects.isNull(pool)) {
			ByteBufferPoolDirect.pool=new DirectByteBufferPool();
		}
	}
	public static DirectByteBufferPool get() {
		return ByteBufferPoolDirect.pool;
	}
}
