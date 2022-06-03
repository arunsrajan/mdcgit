package com.github.mdc.stream;

/**
 * 
 * @author arun
 * This class is a stream with parametered types for pipeline api to process json files.
 * @param <I1>
 */
public final class JsonStream<I1> extends StreamPipeline<I1> {
	@SuppressWarnings({ "rawtypes" })
	public JsonStream(StreamPipeline root) {
		this.root = root;
		this.task = new Json();
		root.childs.add(this);
		this.parents.add(root);
		this.protocol = root.protocol;
	}
}
