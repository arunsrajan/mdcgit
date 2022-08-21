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

import com.github.mdc.common.functions.PeekConsumer;

/**
 * 
 * @author arun
 * This class is a stream with parametered types for pipeline api to process csv files.
 * @param <I1>
 */
public final class CsvStream<I1> extends StreamPipeline<I1> {

	@SuppressWarnings({"rawtypes"})
	public CsvStream(StreamPipeline root, CsvOptions csvOptions) {
		this.root = root;
		this.task = csvOptions;
		root.childs.add(this);
		this.parents.add(root);
		this.protocol = root.protocol;
	}

	@SuppressWarnings({"rawtypes"})
	public CsvStream(StreamPipeline root, PeekConsumer peekconsumer) {
		this.root = root;
		this.task = peekconsumer;
		root.childs.add(this);
		this.parents.add(root);
		this.protocol = root.protocol;
	}

}
