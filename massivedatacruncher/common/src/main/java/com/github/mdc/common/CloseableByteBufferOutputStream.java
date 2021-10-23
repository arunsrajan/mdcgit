package com.github.mdc.common;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

public class CloseableByteBufferOutputStream extends OutputStream {
	static int allocation = 0,deallocation = 0;
	private ByteBuffer bb;
	static Semaphore printallocdealloc = new Semaphore(1);
	static Logger log = Logger.getLogger(CloseableByteBufferOutputStream.class);
	public CloseableByteBufferOutputStream(ByteBuffer bb) {
		try {
			this.bb = bb;
			printallocdealloc.acquire();
			log.info("CloseableByteBufferOutputStream allocated:" + allocation++ + bb);
			
		} 
		catch(InterruptedException ie) {
			log.error(MDCConstants.EMPTY,ie);
			Thread.currentThread().interrupt();
		}
		catch (Exception e) {
			log.error(MDCConstants.EMPTY,e);
		} finally {
			printallocdealloc.release();
		}
		
	}

	@Override
	public synchronized void write(int b) throws IOException {
		bb.put((byte) b);
	}

	@Override
	public synchronized void write(byte[] bytes, int off, int len) throws IOException {
		bb.put(bytes, off, len);
	}
	
	public ByteBuffer get() {
		return bb;
		
	}
	@Override
	public void close() {
		if(!Objects.isNull(bb)) {			
			try {
				printallocdealloc.acquire();
				log.info("CloseableByteBufferOutputStream returning to pool: "+deallocation++ + bb);
				bb.clear();
				bb.rewind();
				GlobalByteBufferSemaphore.get().acquire();
				ByteBufferPool.get().returnObject(bb);
			} catch(InterruptedException ie) {
				log.error(MDCConstants.EMPTY,ie);
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				log.error(MDCConstants.EMPTY,e);
			} finally {
				GlobalByteBufferSemaphore.get().release();
				printallocdealloc.release();
			}
			bb = null;			
		}
	}
}
