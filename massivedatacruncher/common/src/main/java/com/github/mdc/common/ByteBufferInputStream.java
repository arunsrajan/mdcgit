package com.github.mdc.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

public class ByteBufferInputStream extends InputStream {
	static int allocation = 0,deallocation = 0;
	static Logger log = Logger.getLogger(ByteBufferInputStream.class);
	private ByteBuffer bb;
	static Semaphore printallocdealloc = new Semaphore(1);

	public ByteBufferInputStream(ByteBuffer bb) {
		try {
			this.bb = bb;
			printallocdealloc.acquire();
			log.info("ByteBuffer Input Stream allocated:" + allocation++ + bb);
			
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

	public synchronized int read() throws IOException {
		if (!bb.hasRemaining()) {
			return -1;
		}
		return bb.get() & 0xFF;
	}

	public synchronized int read(byte[] bytes, int off, int len) throws IOException {
		if (!bb.hasRemaining()) {
			return -1;
		}
		len = Math.min(len, bb.remaining());
		bb.get(bytes, off, len);
		return len;
	}

	public ByteBuffer get() {
		return bb;
	}

	@Override
	public void close() {
		if(!Objects.isNull(bb)) {			
			try {
				printallocdealloc.acquire();
				log.info("ByteBuffer Input Stream returning to pool: "+deallocation++ + bb);
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
