package com.github.mdc.stream.examples;
import java.net.URI;

import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.stream.StreamPipeline;
import com.github.mdc.stream.Pipeline;

public class StreamReduceRightOuterJoinNormalDivided implements Pipeline {
	private Logger log = Logger.getLogger(StreamReduceRightOuterJoinNormalDivided.class);
	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setIsblocksuserdefined("false");
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setMode(MDCConstants.MODE_NORMAL);
		pipelineconfig.setContaineralloc(MDCConstants.CONTAINER_ALLOC_DIVIDED);
		testMassiveDataCollectExampleRightOuterJoin(args, pipelineconfig);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testMassiveDataCollectExampleRightOuterJoin(String[] args, PipelineConfig pipelineconfig)
			throws Exception {
		log.info("StreamReduceRightOuterJoinNormal.testMassiveDataCollectExampleRightOuterJoin Before---------------------------------------");
		var datastream = StreamPipeline.newStreamHDFS(args[0], args[1], pipelineconfig);
		var mappair1 = datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		var airlinesamples = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		var datastream1 = StreamPipeline.newStreamHDFS(args[0], args[2], pipelineconfig);

		var carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		carriers.rightOuterjoin(airlinesamples, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.saveAsTextFile(new URI(args[0]), args[3] + "/MapRed-" + System.currentTimeMillis());
		log.info("StreamReduceRightOuterJoinNormal.testMassiveDataCollectExampleRightOuterJoin After---------------------------------------");
	}
}
