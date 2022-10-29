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

import java.util.List;

import org.apache.ignite.lang.IgniteCallable;
import org.jgroups.util.UUID;

import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.tasks.executor.Combiner;
import com.github.mdc.tasks.executor.Mapper;

public class IgniteMapperCombiner extends IgniteMapper implements IgniteCallable<MapReduceResult> {
	private static final long serialVersionUID = -4560060224786371070L;
	@SuppressWarnings("rawtypes")
	List<Mapper> crunchmappers;
	@SuppressWarnings("rawtypes")
	List<Combiner> crunchcombiners;

	@SuppressWarnings("rawtypes")
	public IgniteMapperCombiner(BlocksLocation blockslocation, List<Mapper> crunchmappers,
			List<Combiner> crunchcombiners) {
		super(blockslocation, crunchmappers);
		this.crunchcombiners = crunchcombiners;
	}

	@SuppressWarnings({"rawtypes"})
	@Override
	public MapReduceResult call() throws Exception {
		var starttime = System.currentTimeMillis();
		var ctx = super.execute();
		if (crunchcombiners != null && crunchcombiners.size() > 0) {
			var mdcc = new IgniteCombiner(ctx, crunchcombiners.get(0));
			ctx = mdcc.call();
		}
		var mrresult = new MapReduceResult();
		mrresult.cachekey = UUID.randomUUID().toString();
		var cache = ignite.getOrCreateCache(MDCConstants.MDCCACHEMR);
		cache.put(mrresult.cachekey, (DataCruncherContext) ctx);
		var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
		log.info("Time taken to measure mapper task is " + timetaken + " seconds");
		return mrresult;
	}

	public BlocksLocation getBlocksLocation() {
		return this.blockslocation;
	}

}
