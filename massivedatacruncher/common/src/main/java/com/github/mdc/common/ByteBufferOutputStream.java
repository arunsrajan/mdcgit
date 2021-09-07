package com.github.mdc.common;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ByteBufferOutputStream extends OutputStream {
	private ByteBuffer bb;

	public ByteBufferOutputStream(ByteBuffer bb) {
		this.bb = bb;
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
	
}
