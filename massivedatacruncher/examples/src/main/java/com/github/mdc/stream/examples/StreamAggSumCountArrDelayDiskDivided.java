package com.github.mdc.stream.examples;

import java.io.Serializable;
import java.net.URI;

import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.stream.StreamPipeline;
import com.github.mdc.stream.Pipeline;

public class StreamAggSumCountArrDelayDiskDivided implements Serializable, Pipeline {
	private static final long serialVersionUID = 6834009845802448401L;
	private Logger log = Logger.getLogger(StreamAggSumCountArrDelayDiskDivided.class);

	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setStorage(MDCConstants.STORAGE.DISK);
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setMaxmem(args[3]);
		pipelineconfig.setMinmem("512");
		pipelineconfig.setGctype(MDCConstants.ZGC);
		pipelineconfig.setBatchsize(args[4]);
		pipelineconfig.setMode(MDCConstants.MODE_NORMAL);
		pipelineconfig.setContaineralloc(MDCConstants.CONTAINER_ALLOC_DIVIDED);
		testMapValuesReduceByValues(args, pipelineconfig);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testMapValuesReduceByValues(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("testMapValuesReduceByValues Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(args[0], args[1], pipelineconfig);
		datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.mapValues(mv -> new Tuple2<Long, Long>(mv, 1l))
				.reduceByValues((tuple1, tuple2) -> new Tuple2(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.saveAsTextFile(new URI(args[0]), args[2] + "/StreamAggSumCount-" + System.currentTimeMillis());
		log.info("testMapValuesReduceByValues After---------------------------------------");
	}
}
