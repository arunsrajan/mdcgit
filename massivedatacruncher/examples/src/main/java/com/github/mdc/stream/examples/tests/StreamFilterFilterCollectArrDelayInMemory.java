package com.github.mdc.stream.examples.tests;

import java.io.Serializable;
import java.net.URI;

import org.apache.log4j.Logger;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.stream.MassiveDataPipeline;
import com.github.mdc.stream.Pipeline;

public class StreamFilterFilterCollectArrDelayInMemory implements Serializable, Pipeline {
	private static final long serialVersionUID = -1073668309871473457L;
	private Logger log = Logger.getLogger(StreamFilterFilterCollectArrDelayInMemory.class);

	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setStorage(MDCConstants.STORAGE.INMEMORY);
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize(args[5]);
		pipelineconfig.setMaxmem(args[3]);
		pipelineconfig.setMinmem("512");
		pipelineconfig.setGctype(MDCConstants.ZGC);
		pipelineconfig.setBatchsize(args[4]);
		pipelineconfig.setMode(MDCConstants.MODE_NORMAL);
		testMapValuesReduceByValues(args, pipelineconfig);
	}

	@SuppressWarnings({"serial" })
	public void testMapValuesReduceByValues(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("testMapValuesReduceByValues Before---------------------------------------");
		MassiveDataPipeline<String> datastream = MassiveDataPipeline.newStreamHDFS(args[0], args[1], pipelineconfig);
		datastream
		.filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !value.split(",")[14].equals("NA") && !value.split(",")[14].equals("ArrDelay");
			}
		}).filter(new com.github.mdc.stream.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !value.split(",")[14].equals("NA") && !value.split(",")[14].equals("ArrDelay");
			}
		})
		.saveAsTextFile(new URI(args[0]), args[2] + "/FilterFilter-" + System.currentTimeMillis());
		log.info("testMapValuesReduceByValues After---------------------------------------");
	}
}
