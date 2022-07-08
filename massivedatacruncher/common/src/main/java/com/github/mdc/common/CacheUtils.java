package com.github.mdc.common;

import java.time.Duration;
import java.util.Objects;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ConfigurationBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.config.DefaultConfiguration;
import org.ehcache.sizeof.Configuration;
import org.xerial.snappy.SnappyInputStream;

/**
 * 
 * @author arun
 * The cache helper class to initialize and build in memory cache.
 */
public class CacheUtils {
	
	static Logger log = Logger.getLogger(CacheUtils.class);
	
	private CacheUtils() {
	}

	public static enum CacheExpiry {
		HOURS, MINUTES, SECONDS
	};

	/**
	 * This functions builds cache objects given cachename, expiry and dizksize.
	 * @param cachename
	 * @param keytype
	 * @param valuetype
	 * @param sizeingb
	 * @param expiry
	 * @param cacheexpiry
	 * @param disksizeingb
	 * @param cachedatapath
	 * @return cache object
	 */
	@SuppressWarnings("rawtypes")
	public static Cache buildInMemoryCache(String cachename, Class<?> keytype, Class<?> valuetype, int numbuffsize,
			int expiry, CacheExpiry cacheexpiry, int disksizeingb, String cachedatapath) {
		log.debug("Entered CacheUtils.buildInMemoryCache");
		CacheManager cacheManager;
		if (Objects.isNull(MDCCacheManager.get())) {
			CacheConfiguration<?, ?> ccb = CacheConfigurationBuilder
			.newCacheConfigurationBuilder(keytype, valuetype,
					ResourcePoolsBuilder.newResourcePoolsBuilder()
					.heap(numbuffsize, MemoryUnit.MB)
					.disk(disksizeingb, MemoryUnit.GB, false)
							)
			.withExpiry(ExpiryPolicyBuilder
					.timeToLiveExpiration(cacheexpiry == CacheExpiry.HOURS ? Duration.ofHours(expiry)
							: cacheexpiry == CacheExpiry.MINUTES ? Duration.ofMinutes(expiry)
									: Duration.ofSeconds(expiry))).build();
			cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
					.with(CacheManagerBuilder.persistence(cachedatapath)) 
					.withCache(cachename, ccb).build();
			log.debug("Cache Manager Object Built");
			cacheManager.init();
			MDCCacheManager.put(cacheManager);
		} else {
			cacheManager = MDCCacheManager.get();
		}
		log.debug("Cache Manager Object Initialized");
		log.debug("Exiting CacheUtils.buildInMemoryCache");
		return cacheManager.getCache(cachename, keytype, valuetype);
	}

	/**
	 * This function initializes cache for the entire JVM.
	 */
	public static void initCache() {
		log.debug("Entered CacheUtils.initCache");
		String cacheduration = (String) MDCProperties.get().get(MDCConstants.CACHEDURATION);
		MDCCache.put(buildInMemoryCache(MDCConstants.BLOCKCACHE, String.class, byte[].class,
				Integer.parseInt((String) MDCProperties.get().get(MDCConstants.CACHESIZEGB)),
				Integer.parseInt((String) MDCProperties.get().get(MDCConstants.CACHEEXPIRY)),
				CacheUtils.CacheExpiry.valueOf(cacheduration),
				Integer.parseInt((String) MDCProperties.get().get(MDCConstants.CACHEDISKSIZEGB)),
				(String) MDCProperties.get().getProperty(MDCConstants.CACHEDISKPATH, MDCConstants.CACHEDISKPATH_DEFAULT) + MDCConstants.BACKWARD_SLASH + Utils.getUniqueID()));
		log.debug("Exiting CacheUtils.initCache");
	}
	
	/**
	 * This function returns block data in bytes in compressed stream using LZF compression.
	 * @param blockslocation
	 * @param hdfs
	 * @return compressed stream object.
	 * @throws Exception
	 */
	public static SnappyInputStream getBlockData(BlocksLocation blockslocation, FileSystem hdfs) throws Exception {
		log.debug("Entered CacheUtils.getBlockData");
		return HdfsBlockReader.getBlockDataSnappyStream(blockslocation, hdfs);
	
	}
}
