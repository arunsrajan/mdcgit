package com.github.mdc.stream;

import com.github.mdc.stream.functions.PeekConsumer;

/**
 * 
 * @author arun
 * This class is a stream with parametered types for pipeline api to process csv files.
 * @param <I1>
 * @param <I2>
 */
public final class CsvStream<I1, I2> extends StreamPipeline<I1> {

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
