package com.github.mdc.stream.sql;

import java.net.URI;
import java.util.function.IntSupplier;

import com.github.mdc.stream.MapPair;
import com.github.mdc.stream.MassiveDataPipeline;
import com.github.mdc.stream.MassiveDataPipelineException;

public class MDPSql {
	Object mdpmp;
	public MDPSql(Object mdpmp) {
		this.mdpmp = mdpmp;
	}
	
	@SuppressWarnings("rawtypes")
	public Object collect(boolean toexecute, IntSupplier supplier) throws MassiveDataPipelineException {
		if(mdpmp instanceof MassiveDataPipeline mdp) {
			return mdp.collect(toexecute, supplier);
		}
		else if(mdpmp instanceof MapPair mp) {
			return mp.collect(toexecute, supplier);
		}
		return null;
	}
	
	@SuppressWarnings("rawtypes")
	public void  saveAsTextFile(URI uri,String path) throws Exception {
		if(mdpmp instanceof MassiveDataPipeline mdp) {
			mdp.saveAsTextFile(uri, path);
		}
		else if(mdpmp instanceof MapPair mp) {
			mp.saveAsTextFile(uri, path);
		}
	
	}
	
}
