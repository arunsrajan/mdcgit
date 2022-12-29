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

import static java.util.Objects.nonNull;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import org.apache.log4j.Logger;

/**
 * Direct Byte buffer pool which allocates byte buffer
 * 
 * @author arun
 *
 */
public class ByteBufferPoolDirect {

	private static Logger log = Logger.getLogger(ByteBufferPoolDirect.class);

	private static long directmemorysize;
	private static long heapsize;
	private static long memoryallocated = 0;
	private static long totalmemoryallocated = 0;
	private static Semaphore allocatedeallocate = new Semaphore(1);
	private static final int MEMRETRY = 100;

	public static void init() {
		int heappercentage = Integer.parseInt(
				MDCProperties.get().getProperty(MDCConstants.HEAP_PERCENTAGE, MDCConstants.HEAP_PERCENTAGE_DEFAULT));
		long totalmemory = Runtime.getRuntime().maxMemory();
		directmemorysize = totalmemory * (100 - heappercentage) / 100;
		heapsize = totalmemory - directmemorysize;
		log.info("Max Heap Allocated: " + ((heapsize) / MDCConstants.MB) + " MB, Max Direct Memory: "
				+ (directmemorysize / MDCConstants.MB) + " MB");
	}

	public static synchronized ByteBuffer get(long memorytoallocate) throws Exception {
		allocatedeallocate.acquire();
		if (directmemorysize - memoryallocated >= memorytoallocate) {
			totalmemoryallocated += memorytoallocate;
			memoryallocated += memorytoallocate;
			ByteBuffer bb = DirectByteBufferUtil.allocateDirect((int) memorytoallocate);
			if (bb != null) {
				allocatedeallocate.release();
				return bb;
			}
		}
		allocatedeallocate.release();
		ByteBuffer bb = ByteBuffer.allocate((int) memorytoallocate);
		return bb;
	}

	public static void destroy() {
	}

	public static synchronized void destroy(ByteBuffer bb) throws Exception {
		if (nonNull(bb) && bb.isDirect()) {
			allocatedeallocate.acquire();
			memoryallocated -= bb.capacity();
			DirectByteBufferUtil.freeDirectBufferMemory(bb);
			allocatedeallocate.release();
		}
	}

	private ByteBufferPoolDirect() {
	}
}
