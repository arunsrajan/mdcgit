package com.github.mdc.stream.examples;

import java.io.Serializable;
import java.net.URI;

import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.stream.IgnitePipeline;
import com.github.mdc.stream.Pipeline;

public class StreamAggSumCountArrDelayIgnite implements Serializable, Pipeline {
	private static final long serialVersionUID = -1073668309871473457L;
	private Logger log = Logger.getLogger(StreamAggSumCountArrDelayIgnite.class);

	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setLocal("false");
		pipelineconfig.setStorage(MDCConstants.STORAGE.INMEMORY);
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setMaxmem(args[3]);
		pipelineconfig.setMinmem("512");
		pipelineconfig.setGctype(MDCConstants.ZGC);
		pipelineconfig.setBatchsize(args[4]);
		pipelineconfig.setMode(MDCConstants.MODE_DEFAULT);
		testMapValuesReduceByValues(args, pipelineconfig);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testMapValuesReduceByValues(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("testMapValuesReduceByValues Before---------------------------------------");
		IgnitePipeline<String> datastream = IgnitePipeline.newStreamHDFS(args[0], args[1], pipelineconfig);
		datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.mapValues(mv -> new Tuple2<Long, Long>(mv, 1l))
				.reduceByValues((tuple1, tuple2) -> new Tuple2(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.saveAsTextFile(new URI(args[0]), args[2] + "/StreamAggSumCount-" + System.currentTimeMillis());
		log.info("testMapValuesReduceByValues After---------------------------------------");
	}
}
