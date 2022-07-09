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
