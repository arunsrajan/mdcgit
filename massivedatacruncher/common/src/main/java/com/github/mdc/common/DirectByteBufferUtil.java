package com.github.mdc.common;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.misc.Unsafe;

/**
 * Utility which frees the Direct Byte Buffer memory
 * 
 * @author arun
 *
 */
public final class DirectByteBufferUtil {
	
	static Logger log = LoggerFactory.getLogger(DirectByteBufferUtil.class);
	
	static AtomicInteger allocation = new AtomicInteger(1);
	static AtomicInteger deallocation = new AtomicInteger(1);

	protected static Unsafe getUnsafe() {
		try {
			 Field f = Unsafe.class.getDeclaredField("theUnsafe");
             f.setAccessible(true);
             return (Unsafe) f.get(null);
		} catch (Exception ex) {
			return null;
		}
	}

	/**
	 * Frees the specified buffer's direct memory allocation.<br>
	 * The buffer should not be used after calling this method; you should instead
	 * allow it to be garbage-collected by removing all references of it from your
	 * program.
	 * 
	 * @param directBuffer The direct buffer whose memory allocation will be freed
	 * @return Whether or not the memory allocation was freed
	 */
	public static synchronized boolean freeDirectBufferMemory(ByteBuffer directBuffer) {
		if (!directBuffer.isDirect()) {
			return false;
		}
		try {
			Field addressField = Buffer.class.getDeclaredField("address");
			addressField.setAccessible(true);
			//getUnsafe().freeMemory(addressField.getLong(directBuffer));
			getUnsafe().invokeCleaner(directBuffer);
			log.info("Direct Byte Buffer returning to pool deallocation number {} with buffer info {}",
					deallocation.incrementAndGet(), directBuffer);
			return true;
		} catch (Exception ex) {
			ex.printStackTrace();
			return false;
		}
	}

	public static synchronized ByteBuffer allocateDirect(int cap) throws Exception {
		ByteBuffer buffer = ByteBuffer.allocateDirect(cap);
		log.info("Direct Byte Buffer allocation number {} with object info {}", allocation.incrementAndGet(),  buffer);
		return buffer;
	}

}