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
package com.github.mdc.tasks.executor;

import java.io.InputStream;
import java.util.List;

import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.Context;

public class MapperCombinerExecutor extends MapperExecutor {

	BlocksLocation blockslocation;
	@SuppressWarnings("rawtypes")
	List<Mapper> crunchmappers;
	@SuppressWarnings("rawtypes")
	List<Combiner> crunchcombiners;

	@SuppressWarnings("rawtypes")
	public MapperCombinerExecutor(BlocksLocation blockslocation, InputStream datastream, List<Mapper> crunchmappers,
								  List<Combiner> crunchcombiners) {
		super(blockslocation, datastream, crunchmappers);
		this.crunchcombiners = crunchcombiners;
	}

	@SuppressWarnings({"rawtypes"})
	@Override
	public Context call() throws Exception {
		var starttime = System.currentTimeMillis();
		var ctx = super.call();
		if (crunchcombiners != null && crunchcombiners.size() > 0) {
			var mdcc = new CombinerExecutor(ctx, crunchcombiners.get(0));
			ctx = mdcc.call();
			return ctx;
		}
		var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
		log.info("Time taken to guage mapper task is " + timetaken + " seconds");
		return ctx;
	}

}
