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
package com.github.mdc.stream;

import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;

import com.github.mdc.common.functions.AtomicBiConsumer;
import com.github.mdc.common.functions.AtomicIntegerSupplier;
import com.github.mdc.common.functions.AtomicObjIntConsumer;

public class PipelineIntStreamCollect {
	@SuppressWarnings("rawtypes")
	private Supplier supplier;
	@SuppressWarnings("rawtypes")
	private ObjIntConsumer objintconsumer;
	@SuppressWarnings("rawtypes")
	private BiConsumer biconsumer;

	@SuppressWarnings("rawtypes")
	public PipelineIntStreamCollect(AtomicIntegerSupplier supplier,
			AtomicObjIntConsumer objintconsumer,
			AtomicBiConsumer biconsumer) {
		this.supplier = supplier;
		this.objintconsumer = objintconsumer;
		this.biconsumer = biconsumer;
	}

	@SuppressWarnings("rawtypes")
	public Supplier getSupplier() {
		return this.supplier;
	}

	@SuppressWarnings("rawtypes")
	public ObjIntConsumer getObjIntConsumer() {
		return this.objintconsumer;
	}

	@SuppressWarnings("rawtypes")
	public BiConsumer getBiConsumer() {
		return this.biconsumer;
	}
}
