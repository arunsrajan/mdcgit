package com.github.mdc.common;

import java.io.ByteArrayOutputStream;
import java.util.Objects;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class ByteArrayOutputStreamPool {
	private static GenericObjectPool<ByteArrayOutputStream> pool = null;
	public static void init(int maxpoolsize) {
		if(Objects.isNull(pool)||pool.isClosed()) {
			var config = new GenericObjectPoolConfig<ByteArrayOutputStream>();
			config.setMaxTotal(maxpoolsize);
			config.setBlockWhenExhausted(true);
			config.setTestOnBorrow(true);
	        config.setTestOnReturn(true);
			var factory = new ByteArrayOutputStreamFactory(); 
			ByteArrayOutputStreamPool.pool=new GenericObjectPool<ByteArrayOutputStream>(factory, config);
			try {
				pool.preparePool();
			} catch (Exception e) {
			}
		}
	}
	public static GenericObjectPool<ByteArrayOutputStream> get() {
		return ByteArrayOutputStreamPool.pool;
	}
	
	
}
