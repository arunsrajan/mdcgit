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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

public class CloseableByteBufferOutputStream extends OutputStream {
	static int allocation,deallocation;
	private ByteBuffer bb;
	static Semaphore printallocdealloc = new Semaphore(1);
	static Logger log = Logger.getLogger(CloseableByteBufferOutputStream.class);

	public CloseableByteBufferOutputStream(ByteBuffer bb) {
		try {
			this.bb = bb;
			printallocdealloc.acquire();
			log.info("CloseableByteBufferOutputStream allocated:" + allocation++ + bb);

		}
		catch (InterruptedException ie) {
			log.error(MDCConstants.EMPTY, ie);
			Thread.currentThread().interrupt();
		}
		catch (Exception e) {
			log.error(MDCConstants.EMPTY, e);
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
		if (!Objects.isNull(bb)) {
			try {
				log.info("CloseableByteBufferOutputStream returning to pool: " + deallocation++ + bb);
				printallocdealloc.acquire();
				bb.clear();
				bb.rewind();
				ByteBufferPool.get().returnObject(bb);
				log.info("CloseableByteBufferOutputStream returning to pool deallocated: " + bb);
			} catch (InterruptedException ie) {
				log.error(MDCConstants.EMPTY, ie);
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				log.error(MDCConstants.EMPTY, e);
			} finally {
				printallocdealloc.release();
			}
			bb = null;
		}
	}
}
