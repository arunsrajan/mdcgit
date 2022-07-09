package com.github.mdc.common;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class ByteBufferPool {
	private static GenericObjectPool<ByteBuffer> pool;

	public static void init(int maxpoolsize) {
		if (Objects.isNull(pool) || pool.isClosed()) {
			var config = new GenericObjectPoolConfig<ByteBuffer>();
			config.setMinIdle(1);
			config.setMaxIdle(maxpoolsize);
			config.setMaxTotal(maxpoolsize);
			config.setBlockWhenExhausted(Boolean.parseBoolean(
					(String) MDCProperties.get()
							.getProperty(MDCConstants.BYTEBUFFERPOOL_BLOCK
							, MDCConstants.BYTEBUFFERPOOL_BLOCK_DEFAULT)));
			var factory = new ByteBufferFactory();
			ByteBufferPool.pool = new GenericObjectPool<ByteBuffer>(factory, config);
		}
	}

	public static GenericObjectPool<ByteBuffer> get() {
		return ByteBufferPool.pool;
	}

	private ByteBufferPool() {
	}


}
