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

public class StreamReduceNormalInMemoryImplicit implements Serializable, Pipeline {
	private static final long serialVersionUID = -681856073825969510L;
	private Logger log = Logger.getLogger(StreamReduceNormalInMemoryImplicit.class);

	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setIsblocksuserdefined("false");
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setMode(MDCConstants.MODE_NORMAL);
		pipelineconfig.setContaineralloc(MDCConstants.CONTAINER_ALLOC_IMPLICIT);
		pipelineconfig.setImplicitcontainerallocanumber(args[4]);
		pipelineconfig.setImplicitcontainercpu(args[5]);
		pipelineconfig.setImplicitcontainermemory("MB");
		pipelineconfig.setImplicitcontainermemorysize(args[6]);
		testReduce(args, pipelineconfig);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testReduce(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("StreamReduceNormalInMemoryImplicit.testReduce Before---------------------------------------");
		var datastream = StreamPipeline.newStreamHDFS(args[0], args[1], pipelineconfig);
		var mappair1 = datastream.map(dat -> dat.split(","))
				.filter(dat -> !dat[14].equals("ArrDelay") && !dat[14].equals("NA"))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		var airlinesamples = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		var datastream1 = StreamPipeline.newStreamHDFS(args[0], args[2], pipelineconfig);

		var carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		carriers.join(airlinesamples, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.saveAsTextFile(new URI(args[0]), args[3] + "/StreamOutReduce-" + System.currentTimeMillis());
		log.info("StreamReduceNormalInMemoryImplicit.testReduce After---------------------------------------");
	}
}
