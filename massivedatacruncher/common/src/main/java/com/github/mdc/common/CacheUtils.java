/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.mdc.common;

import java.io.InputStream;
import java.time.Duration;
import java.util.Objects;
import org.apache.hadoop.fs.FileSystem;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author arun The cache helper class to initialize and build in memory cache.
 */
public class CacheUtils {

  static org.slf4j.Logger log = LoggerFactory.getLogger(CacheUtils.class);

  private CacheUtils() {}

  public enum CacheExpiry {
    HOURS, MINUTES, SECONDS
  }

  /**
   * This functions builds cache objects given cachename, expiry and dizksize.
   * 
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
  public static Cache<?, ?> buildInMemoryCache(String cachename, Class<?> keytype,
      Class<?> valuetype, int numbuffsize, int expiry, CacheExpiry cacheexpiry, int disksizeingb,
      String cachedatapath) {
    log.debug("Entered CacheUtils.buildInMemoryCache");
    CacheManager cacheManager;
    CacheConfiguration<?, ?> ccb = CacheConfigurationBuilder
        .newCacheConfigurationBuilder(keytype, valuetype,
            ResourcePoolsBuilder.newResourcePoolsBuilder().heap(numbuffsize, MemoryUnit.MB)
                .disk(disksizeingb, MemoryUnit.GB, false))
        .withExpiry(ExpiryPolicyBuilder
            .timeToLiveExpiration(cacheexpiry == CacheExpiry.HOURS ? Duration.ofHours(expiry)
                : cacheexpiry == CacheExpiry.MINUTES ? Duration.ofMinutes(expiry)
                    : Duration.ofSeconds(expiry)))
        .build();
    if (Objects.isNull(MDCCacheManager.get())) {
      cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
          .with(CacheManagerBuilder.persistence(cachedatapath)).build();
      log.debug("Cache Manager Object Built");
      cacheManager.init();
      MDCCacheManager.put(cacheManager);
    } else {
      cacheManager = MDCCacheManager.get();
    }
    try {
      return cacheManager.createCache(cachename, ccb);
    } catch (Exception ex) {
      log.info("Cache {}  Already Available, so obtaining cache... ", cachename);
      return cacheManager.getCache(cachename, keytype, valuetype);
    }
  }

  /**
   * This function initializes cache for the entire JVM.
   */
  public static void initCache() {
    log.debug("Entered CacheUtils.initCache");
    String cacheduration = (String) MDCProperties.get().get(MDCConstants.CACHEDURATION);
    MDCCache
        .put(buildInMemoryCache(MDCConstants.BLOCKCACHE, String.class, byte[].class,
            Integer.parseInt((String) MDCProperties.get().get(MDCConstants.CACHESIZEGB)),
            Integer.parseInt((String) MDCProperties.get().get(MDCConstants.CACHEEXPIRY)),
            CacheUtils.CacheExpiry.valueOf(cacheduration),
            Integer.parseInt((String) MDCProperties.get().get(MDCConstants.CACHEDISKSIZEGB)),
            (String) MDCProperties.get().getProperty(MDCConstants.CACHEDISKPATH,
                MDCConstants.CACHEDISKPATH_DEFAULT) + MDCConstants.FORWARD_SLASH
                + Utils.getCacheID()));
    log.debug("Exiting CacheUtils.initCache");
  }

  /**
   * Initializes Block metadata cache
   */
  @SuppressWarnings("unchecked")
  public static void initBlockMetadataCache() {
    log.debug("Entered CacheUtils.initBlockMetadataCache");
    String cacheduration = (String) MDCProperties.get().get(MDCConstants.CACHEDURATION);
    MDCCache.putBlocksMetadata((Cache<String, BlocksLocation[]>) buildInMemoryCache(
        MDCConstants.BLOCKSLOCATIONMETADATACACHE, String.class, BlocksLocation[].class,
        Integer.parseInt((String) MDCProperties.get().get(MDCConstants.CACHESIZEGB)),
        Integer.parseInt((String) MDCProperties.get().get(MDCConstants.CACHEEXPIRY)),
        CacheUtils.CacheExpiry.valueOf(cacheduration),
        Integer.parseInt((String) MDCProperties.get().get(MDCConstants.CACHEDISKSIZEGB)),
        (String) MDCProperties.get().getProperty(MDCConstants.CACHEBLOCKSLOCATIONDISKPATH,
            MDCConstants.CACHEBLOCKSLOCATIONDISKPATH_DEFAULT) + MDCConstants.FORWARD_SLASH
            + Utils.getCacheID()));
    MDCCache
        .putFileMetadata((Cache<String, String>) buildInMemoryCache(MDCConstants.FILEMETADATACACHE,
            String.class, String.class,
            Integer.parseInt((String) MDCProperties.get().get(MDCConstants.CACHESIZEGB)),
            Integer.parseInt((String) MDCProperties.get().get(MDCConstants.CACHEEXPIRY)),
            CacheUtils.CacheExpiry.valueOf(cacheduration),
            Integer.parseInt((String) MDCProperties.get().get(MDCConstants.CACHEDISKSIZEGB)),
            (String) MDCProperties.get().getProperty(MDCConstants.CACHEFILEMETDATADISKPATH,
                MDCConstants.CACHEFILEMETDATADISKPATH_DEFAULT) + MDCConstants.FORWARD_SLASH
                + Utils.getCacheID()));
    log.debug("Exiting CacheUtils.initBlockMetadataCache");
  }


  /**
   * This function returns block data in bytes in compressed stream using LZF compression.
   * 
   * @param blockslocation
   * @param hdfs
   * @return compressed stream object.
   * @throws Exception
   */
  public static InputStream getBlockData(BlocksLocation blockslocation, FileSystem hdfs)
      throws Exception {
    log.debug("Entered CacheUtils.getBlockData");
    return HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);

  }
}
