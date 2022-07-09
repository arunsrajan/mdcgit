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

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.log4j.Logger;

public class ByteBufferFactory extends BasePooledObjectFactory<ByteBuffer> {
	static Logger log = Logger.getLogger(ByteBufferFactory.class);

	@Override
	public ByteBuffer create() throws Exception {
		try {
			ByteBuffer bb = ByteBuffer.allocateDirect(128 * MDCConstants.MB);
			return bb;
		} catch (Exception ex) {
			log.error(MDCConstants.EMPTY, ex);
			return null;
		}

	}

	@Override
	public PooledObject<ByteBuffer> wrap(ByteBuffer bbf) {
		return new DefaultPooledObject<ByteBuffer>(bbf);
	}

	@Override
	public void passivateObject(PooledObject<ByteBuffer> pooledObject) {

	}

	@Override
	public void destroyObject(PooledObject<ByteBuffer> poolobj) throws Exception {
	}
}
