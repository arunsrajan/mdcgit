package com.github.mdc.common;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

public class CloseableByteBufferOutputStream extends OutputStream {
	private ByteBuffer bb;

	public CloseableByteBufferOutputStream(ByteBuffer bb) {
		this.bb = bb;
		bb.clear();
		bb.rewind();
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
				bb.clear();
				bb.rewind();
				ByteBufferPool.get().returnObject(bb);
				ByteBufferPoolDirect.get().give(bb);
			} catch (Exception e) {
			} finally {
			}
			bb = null;			
		}
	}
}
