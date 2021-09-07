package com.github.mdc.common;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import sun.misc.Unsafe;

public class ByteArrayOutputStreamFactory extends BasePooledObjectFactory<ByteArrayOutputStream> {

	public static final Integer ALLOCATEMEMSIZE = 128*1024*1024;	
	
	@Override
	public ByteArrayOutputStream create() throws Exception {
		return new ByteArrayOutputStream(ALLOCATEMEMSIZE);
	}

	@Override
	public PooledObject<ByteArrayOutputStream> wrap(ByteArrayOutputStream bbf) {
		return new DefaultPooledObject<ByteArrayOutputStream>(bbf);
	}
	
	@Override
    public void passivateObject(PooledObject<ByteArrayOutputStream> pooledObject) {
        pooledObject.getObject().reset();
    }
	
	@Override
	public void destroyObject(PooledObject<ByteArrayOutputStream> poolobj) throws Exception {
	}
}
