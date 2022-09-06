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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ByteBufferInputStream extends InputStream {
	static int allocation;
	static int deallocation;
	static Logger log = LoggerFactory.getLogger(ByteBufferInputStream.class);
	private ByteBuffer bb;
	static Semaphore printallocdealloc = new Semaphore(1);

	public ByteBufferInputStream(ByteBuffer bb) {
		try {
			this.bb = bb;
			printallocdealloc.acquire();
			log.info("ByteBuffer Input Stream allocation number {} with object info {}", allocation++,  bb);

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

	public synchronized int read() throws IOException {
		if (!bb.hasRemaining()) {
			return -1;
		}
		return bb.get() & 0xFF;
	}

	@Override
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
		if (!Objects.isNull(bb)) {
			try {
				printallocdealloc.acquire();
				log.info("ByteBuffer Input Stream returning to pool deallocation number {} with buffer info {}", deallocation++,  bb);
				bb.clear();
				bb.rewind();
				ByteBufferPoolDirect.get().give(bb);
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
