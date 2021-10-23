package com.github.mdc.common;

import java.nio.ByteBuffer;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class ByteBufferFactory extends BasePooledObjectFactory<ByteBuffer> {

	@Override
	public ByteBuffer create() throws Exception {
		GlobalByteBufferSemaphore.get().acquire();
		ByteBuffer bb = ByteBufferPoolDirect.get().take(128*MDCConstants.MB);
		GlobalByteBufferSemaphore.get().release();
		return bb;
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
