package com.github.mdc.common;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteBufferPoolDirectTest {
	static Logger log = LoggerFactory.getLogger(ByteBufferPoolDirectTest.class);
	@BeforeClass
	public static void initCache() throws Exception {
		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, MDCConstants.MDC_TEST_PROPERTIES);
		CacheUtils.initCache();
		ByteBufferPoolDirect.init();
	}
	@Test
	public void testByteBufferPool() throws Exception {
		int numiteration = 1000;
		int count = 0;
		Random rand = new Random(System.currentTimeMillis());
		List<Thread> threads = new Vector<>();
		while(count<numiteration) {
			Thread thr = new Thread(() -> {
				ByteBuffer bf = null;
			try {
				bf = ByteBufferPoolDirect.get(128*1024*1024);
				log.info(""+bf+" is Direct: "+bf.isDirect());
				Thread.sleep(rand.nextLong(1000));
			} catch (Exception e) {
				e.printStackTrace();
			}
			finally {
				try {
					log.info("Destroying Byte Buffer:"+bf);
					ByteBufferPoolDirect.destroy(bf);
					log.info("Destroyed Byte Buffer:"+bf);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			});
			threads.add(thr);
			thr.start();
			count++;
		}
		while(true) {
			if(threads.size()==0)break;
			Thread thr = threads.remove(0);
			thr.join();
		}
	}
	@AfterClass
	public static void destroyCache() throws Exception {
		MDCCache.get().clear();
		MDCCacheManager.get().close();
		ByteBufferPoolDirect.destroy();
	}
}
