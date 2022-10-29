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
package com.github.mdc.tasks.scheduler.ignite;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.log4j.Logger;
import org.xerial.snappy.SnappyInputStream;

import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.Context;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.tasks.executor.Mapper;

@SuppressWarnings("rawtypes")
public class IgniteMapper {
	@IgniteInstanceResource
	Ignite ignite;

	public IgniteMapper() {
	}

	static Logger log = Logger.getLogger(IgniteMapper.class);
	BlocksLocation blockslocation;
	List<Mapper> crunchmappers;

	public IgniteMapper(BlocksLocation blockslocation, List<Mapper> crunchmappers) {
		this.blockslocation = blockslocation;
		this.crunchmappers = crunchmappers;
	}

	public Context execute() throws Exception {
		try (IgniteCache<Object, byte[]> cache = ignite.getOrCreateCache(MDCConstants.MDCCACHE);
				var compstream = new SnappyInputStream(new ByteArrayInputStream(cache.get(blockslocation)));
				var br =
						new BufferedReader(new InputStreamReader(compstream));) {
			var ctx = new DataCruncherContext();
			br.lines().parallel().forEach(line -> {
				for (Mapper crunchmapper : crunchmappers) {
					crunchmapper.map(0l, line, ctx);
				}
			});
			return ctx;
		}
		catch (Exception ex) {
			log.error(MDCConstants.EMPTY, ex);
			throw ex;
		}

	}


}
