package com.github.mdc.stream;

import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;

import com.github.mdc.stream.functions.AtomicBiConsumer;
import com.github.mdc.stream.functions.AtomicIntegerSupplier;
import com.github.mdc.stream.functions.AtomicObjIntConsumer;

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
