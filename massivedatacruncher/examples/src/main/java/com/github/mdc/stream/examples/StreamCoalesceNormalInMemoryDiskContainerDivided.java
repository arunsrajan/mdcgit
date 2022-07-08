package com.github.mdc.stream.examples;

import java.io.Serializable;
import java.net.URI;

import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.stream.Pipeline;
import com.github.mdc.stream.StreamPipeline;

public class StreamCoalesceNormalInMemoryDiskContainerDivided implements Serializable, Pipeline {
	private static final long serialVersionUID = -7001849661976107123L;
	private Logger log = Logger.getLogger(StreamCoalesceNormalInMemoryDiskContainerDivided.class);

	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setStorage(MDCConstants.STORAGE.INMEMORY_DISK);
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setGctype(MDCConstants.ZGC);
		pipelineconfig.setMode(MDCConstants.NORMAL);
		pipelineconfig.setNumberofcontainers("3");
		pipelineconfig.setBatchsize(args[3]);
		pipelineconfig.setContaineralloc("DIVIDED");
		testReduce(args, pipelineconfig);
	}

	public void testReduce(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("StreamCoalesceNormalInMemoryDiskContainerDivided.testReduce Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(args[0], args[1],
				pipelineconfig);
		datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.mapValues(mv -> new Tuple2<Long, Long>(mv, 1l))
				.reduceByValues((tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.saveAsTextFile(new URI(args[0]), args[2] + "/StreamOutReduce-" + System.currentTimeMillis());
		log.info("StreamCoalesceNormalInMemoryDiskContainerDivided.testReduce After---------------------------------------");
	}
}
