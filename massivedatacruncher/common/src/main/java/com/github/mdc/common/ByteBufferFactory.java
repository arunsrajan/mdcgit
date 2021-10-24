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
			GlobalByteBufferSemaphore.get().acquire();
			ByteBuffer bb = ByteBufferPoolDirect.get().take(128 * MDCConstants.MB);
			return bb;
		} catch (Exception ex) {
			log.error(MDCConstants.EMPTY, ex);
			return null;
		} finally {
			GlobalByteBufferSemaphore.get().release();
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
