package com.github.mdc.common;

import java.util.concurrent.Semaphore;

public class GlobalByteBufferSemaphore {
	static private Semaphore sembb = new Semaphore(1);
	
	public static Semaphore get() {
		return sembb;
	}
	
}
