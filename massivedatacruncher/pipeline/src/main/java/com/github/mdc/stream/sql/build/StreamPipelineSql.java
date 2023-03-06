package com.github.mdc.stream.sql.build;

import java.net.URI;
import java.util.function.IntSupplier;

import com.github.mdc.stream.MapPair;
import com.github.mdc.stream.PipelineException;
import com.github.mdc.stream.StreamPipeline;

public class StreamPipelineSql {
	Object mdpmp;

	public StreamPipelineSql(Object mdpmp) {
		this.mdpmp = mdpmp;
	}

	@SuppressWarnings("rawtypes")
	public Object collect(boolean toexecute, IntSupplier supplier) throws PipelineException {
		if (mdpmp instanceof StreamPipeline mdp) {
			return mdp.collect(toexecute, supplier);
		}
		else if (mdpmp instanceof MapPair mp) {
			return mp.collect(toexecute, supplier);
		}
		return mdpmp;
	}
	
	@SuppressWarnings("rawtypes")
	public void  saveAsTextFile(URI uri, String path) throws Exception {
		if (mdpmp instanceof StreamPipeline mdp) {
			mdp.saveAsTextFile(uri, path);
		}
		else if (mdpmp instanceof MapPair mp) {
			mp.saveAsTextFile(uri, path);
		}

	}

}

