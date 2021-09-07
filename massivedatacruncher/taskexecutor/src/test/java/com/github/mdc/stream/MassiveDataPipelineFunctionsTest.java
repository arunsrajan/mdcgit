package com.github.mdc.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.function.ToIntFunction;

import org.jooq.lambda.tuple.Tuple2;
import org.junit.Test;

import com.github.mdc.common.DAGEdge;
import com.github.mdc.common.Job;
import com.github.mdc.common.JobMetrics;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.stream.MapPair;
import com.github.mdc.stream.MassiveDataPipeline;
import com.github.mdc.stream.PipelineIntStream;
import com.github.mdc.stream.SampleSupplierInteger;
import com.github.mdc.stream.functions.Distinct;
import com.github.mdc.stream.functions.IntersectionFunction;
import com.github.mdc.stream.functions.KeyByFunction;
import com.github.mdc.stream.functions.LeftOuterJoinPredicate;
import com.github.mdc.stream.functions.MapFunction;
import com.github.mdc.stream.functions.MapToPairFunction;
import com.github.mdc.stream.functions.PeekConsumer;
import com.github.mdc.stream.functions.PredicateSerializable;
import com.github.mdc.stream.functions.RightOuterJoinPredicate;
import com.github.mdc.stream.functions.SToIntFunction;
import com.github.mdc.stream.functions.SortedComparator;
import com.github.mdc.stream.functions.UnionFunction;

public class MassiveDataPipelineFunctionsTest extends MassiveDataPipelineBaseTestClassCommon {
	PipelineConfig pipelineconfig = new PipelineConfig();

	@Test
	public void testMassiveDataPipelineMap() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		MapFunction<String, String> mapfunction = data -> data;
		MassiveDataPipeline<String> map = mdp.map(mapfunction);
		assertTrue(map.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == map);
		assertTrue(map.task == mapfunction);
		assertTrue(map.root == mdp);
	}

	@Test
	public void testMassiveDataPipelineMapFormDAGAbsFunc() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		MapFunction<String, String> mapfunction = data -> data;
		MassiveDataPipeline<String> mdpchild = mdp.map(mapfunction);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineFilter() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		PredicateSerializable<String> filterfunction = data -> data.equals(data);
		MassiveDataPipeline<String> filter = mdp.filter(filterfunction);
		assertTrue(filter.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == filter);
		assertTrue(filter.task == filterfunction);
		assertTrue(filter.root == mdp);
	}

	@Test
	public void testMassiveDataPipelineFilterFormDAGAbsFunc() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		PredicateSerializable<String> filterfunction = data -> data.equals(data);
		MassiveDataPipeline<String> mdpchild = mdp.filter(filterfunction);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelinePeekConsumer() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		PeekConsumer<String> peekconsumer = System.out::println;
		MassiveDataPipeline<String> peek = mdp.peek(peekconsumer);
		assertTrue(peek.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == peek);
		assertTrue(peek.task == peekconsumer);
		assertTrue(peek.root == mdp);
	}

	@Test
	public void testMassiveDataPipelinePeekConsumerFormDAGAbsFunc() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		PeekConsumer<String> peekconsumer = System.out::println;
		MassiveDataPipeline<String> mdpchild = mdp.peek(peekconsumer);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineUnion() throws Exception {
		MassiveDataPipeline<String> mdpunion1 = new MassiveDataPipeline<String>();
		MassiveDataPipeline<String> mdpunion2 = new MassiveDataPipeline<String>();
		MassiveDataPipeline<String> union = mdpunion1.union(mdpunion2);
		assertTrue(union.parents.get(0) == mdpunion1);
		assertTrue(mdpunion1.childs.get(0) == union);
		assertTrue(union.parents.get(1) == mdpunion2);
		assertTrue(mdpunion2.childs.get(0) == union);
		assertTrue(union.task instanceof UnionFunction);
	}

	@Test
	public void testMassiveDataPipelineUnionFormDAGAbsFunc() throws Exception {
		MassiveDataPipeline<String> mdpleft = new MassiveDataPipeline<String>();
		MassiveDataPipeline<String> mdpright = new MassiveDataPipeline<String>();
		MassiveDataPipeline<String> mdpchild = mdpleft.union(mdpright);
		mdpleft.graph.addVertex(mdpleft);
		mdpleft.formDAGAbstractFunction(mdpleft, mdpleft.childs);
		assertEquals(2, mdpleft.graph.vertexSet().size());
		assertEquals(1, mdpleft.graph.edgeSet().size());
		assertTrue(mdpleft.graph.vertexSet().contains(mdpleft));
		assertTrue(mdpleft.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdpleft.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpleft);
		assertTrue(dagedge.getTarget() == mdpchild);

		mdpright.graph.addVertex(mdpright);
		mdpright.formDAGAbstractFunction(mdpright, mdpright.childs);
		assertEquals(2, mdpright.graph.vertexSet().size());
		assertEquals(1, mdpright.graph.edgeSet().size());
		assertTrue(mdpright.graph.vertexSet().contains(mdpright));
		assertTrue(mdpright.graph.vertexSet().contains(mdpchild));
		dagedge = mdpright.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpright);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineIntersection() throws Exception {
		MassiveDataPipeline<String> mdpintersection1 = new MassiveDataPipeline<String>();
		MassiveDataPipeline<String> mdpintersection2 = new MassiveDataPipeline<String>();
		MassiveDataPipeline<String> intersection = mdpintersection1.intersection(mdpintersection2);
		assertTrue(intersection.parents.get(0) == mdpintersection1);
		assertTrue(mdpintersection1.childs.get(0) == intersection);
		assertTrue(intersection.parents.get(1) == mdpintersection2);
		assertTrue(mdpintersection2.childs.get(0) == intersection);
		assertTrue(intersection.task instanceof IntersectionFunction);
	}

	@Test
	public void testMassiveDataPipelineIntersectionFormDAGAbsFunc() throws Exception {
		MassiveDataPipeline<String> mdpleft = new MassiveDataPipeline<String>();
		MassiveDataPipeline<String> mdpright = new MassiveDataPipeline<String>();
		MassiveDataPipeline<String> mdpchild = mdpleft.intersection(mdpright);
		mdpleft.graph.addVertex(mdpleft);
		mdpleft.formDAGAbstractFunction(mdpleft, mdpleft.childs);
		assertEquals(2, mdpleft.graph.vertexSet().size());
		assertEquals(1, mdpleft.graph.edgeSet().size());
		assertTrue(mdpleft.graph.vertexSet().contains(mdpleft));
		assertTrue(mdpleft.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdpleft.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpleft);
		assertTrue(dagedge.getTarget() == mdpchild);

		mdpright.graph.addVertex(mdpright);
		mdpright.formDAGAbstractFunction(mdpright, mdpright.childs);
		assertEquals(2, mdpright.graph.vertexSet().size());
		assertEquals(1, mdpright.graph.edgeSet().size());
		assertTrue(mdpright.graph.vertexSet().contains(mdpright));
		assertTrue(mdpright.graph.vertexSet().contains(mdpchild));
		dagedge = mdpright.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpright);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineMapPair() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		MapToPairFunction<String, Tuple2<String, String>> mappairfunction = (data) -> new Tuple2<>(data, data);
		MapPair<String, String> mappair = mdp.mapToPair(mappairfunction);
		assertTrue(mappair.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == mappair);
		assertTrue(mappair.task == mappairfunction);
	}

	@Test
	public void testMassiveDataPipelineMapPairFormDAGAbsFunc() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		MapToPairFunction<String, Tuple2<String, String>> mappairfunction = (data) -> new Tuple2<>(data, data);
		MapPair<String, String> mdpchild = mdp.mapToPair(mappairfunction);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineSample() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		Integer samplenumber = 10;
		MassiveDataPipeline<String> sample = mdp.sample(samplenumber);
		assertTrue(sample.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == sample);
		assertTrue(sample.task instanceof SampleSupplierInteger);
		SampleSupplierInteger samplesuppinteger = (SampleSupplierInteger) sample.task;
		assertTrue(samplesuppinteger.getAsInt() == samplenumber);
	}

	@Test
	public void testMassiveDataPipelineSampleFormDAGAbsFunc() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		Integer samplenumber = 10;
		MassiveDataPipeline<String> mdpchild = mdp.sample(samplenumber);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineRightOuterJoin() throws Exception {
		MassiveDataPipeline<String> mdpleft = new MassiveDataPipeline<String>();
		MassiveDataPipeline<String> mdpright = new MassiveDataPipeline<String>();
		RightOuterJoinPredicate<String, String> roj = (datleft, datright) -> datleft.equals(datright);
		MassiveDataPipeline<String> mdpchild = mdpleft.rightOuterjoin(mdpright, roj);
		assertTrue(mdpchild.parents.get(0) == mdpleft);
		assertTrue(mdpleft.childs.get(0) == mdpchild);
		assertTrue(mdpchild.parents.get(1) == mdpright);
		assertTrue(mdpright.childs.get(0) == mdpchild);
		assertTrue(mdpchild.task instanceof RightOuterJoinPredicate);
		assertTrue(mdpchild.task == roj);
	}

	@Test
	public void testMassiveDataPipelineRightOuterJoinFormDAGAbsFunc() throws Exception {
		MassiveDataPipeline<String> mdpleft = new MassiveDataPipeline<String>();
		MassiveDataPipeline<String> mdpright = new MassiveDataPipeline<String>();
		RightOuterJoinPredicate<String, String> roj = (datleft, datright) -> datleft.equals(datright);
		MassiveDataPipeline<String> mdpchild = mdpleft.rightOuterjoin(mdpright, roj);
		mdpleft.graph.addVertex(mdpleft);
		mdpleft.formDAGAbstractFunction(mdpleft, mdpleft.childs);
		assertEquals(2, mdpleft.graph.vertexSet().size());
		assertEquals(1, mdpleft.graph.edgeSet().size());
		assertTrue(mdpleft.graph.vertexSet().contains(mdpleft));
		assertTrue(mdpleft.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdpleft.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpleft);
		assertTrue(dagedge.getTarget() == mdpchild);

		mdpright.graph.addVertex(mdpright);
		mdpright.formDAGAbstractFunction(mdpright, mdpright.childs);
		assertEquals(2, mdpright.graph.vertexSet().size());
		assertEquals(1, mdpright.graph.edgeSet().size());
		assertTrue(mdpright.graph.vertexSet().contains(mdpright));
		assertTrue(mdpright.graph.vertexSet().contains(mdpchild));
		dagedge = mdpright.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpright);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineLeftOuterJoin() throws Exception {
		MassiveDataPipeline<String> mdpleft = new MassiveDataPipeline<String>();
		MassiveDataPipeline<String> mdpright = new MassiveDataPipeline<String>();
		LeftOuterJoinPredicate<String, String> loj = (datleft, datright) -> datleft.equals(datright);
		MassiveDataPipeline<String> mdpchild = mdpleft.leftOuterjoin(mdpright, loj);
		assertTrue(mdpchild.parents.get(0) == mdpleft);
		assertTrue(mdpleft.childs.get(0) == mdpchild);
		assertTrue(mdpchild.parents.get(1) == mdpright);
		assertTrue(mdpright.childs.get(0) == mdpchild);
		assertTrue(mdpchild.task instanceof LeftOuterJoinPredicate);
		assertTrue(mdpchild.task == loj);
	}

	@Test
	public void testMassiveDataPipelineLeftOuterJoinFormDAGAbsFunc() throws Exception {
		MassiveDataPipeline<String> mdpleft = new MassiveDataPipeline<String>();
		MassiveDataPipeline<String> mdpright = new MassiveDataPipeline<String>();
		LeftOuterJoinPredicate<String, String> loj = (datleft, datright) -> datleft.equals(datright);
		MassiveDataPipeline<String> mdpchild = mdpleft.leftOuterjoin(mdpright, loj);
		mdpleft.graph.addVertex(mdpleft);
		mdpleft.formDAGAbstractFunction(mdpleft, mdpleft.childs);
		assertEquals(2, mdpleft.graph.vertexSet().size());
		assertEquals(1, mdpleft.graph.edgeSet().size());
		assertTrue(mdpleft.graph.vertexSet().contains(mdpleft));
		assertTrue(mdpleft.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdpleft.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpleft);
		assertTrue(dagedge.getTarget() == mdpchild);

		mdpright.graph.addVertex(mdpright);
		mdpright.formDAGAbstractFunction(mdpright, mdpright.childs);
		assertEquals(2, mdpright.graph.vertexSet().size());
		assertEquals(1, mdpright.graph.edgeSet().size());
		assertTrue(mdpright.graph.vertexSet().contains(mdpright));
		assertTrue(mdpright.graph.vertexSet().contains(mdpchild));
		dagedge = mdpright.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpright);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineSorted() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		SortedComparator<String> sortedcomparator = (dat1, dat2) -> dat1.compareTo(dat2);
		MassiveDataPipeline<String> mdpchild = mdp.sorted(sortedcomparator);
		assertTrue(mdpchild.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == mdpchild);
		assertTrue(mdpchild.task instanceof SortedComparator);
		assertTrue(mdpchild.task == sortedcomparator);
	}

	@Test
	public void testMassiveDataPipelineSortedFormDAGAbsFunc() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		SortedComparator<String> sortedcomparator = (dat1, dat2) -> dat1.compareTo(dat2);
		MassiveDataPipeline<String> mdpchild = mdp.sorted(sortedcomparator);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineDistinct() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		MassiveDataPipeline<String> mdpchild = mdp.distinct();
		assertTrue(mdpchild.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == mdpchild);
		assertTrue(mdpchild.task instanceof Distinct);
	}

	@Test
	public void testMassiveDataPipelineDistinctFormDAGAbsFunc() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		MassiveDataPipeline<String> mdpchild = mdp.distinct();
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineMaptoInt() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		SToIntFunction<String> tointfunction = (dat) -> Integer.parseInt(dat);
		PipelineIntStream<String> mdpchild = mdp.mapToInt(tointfunction);
		assertTrue(mdpchild.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == mdpchild);
		assertTrue(mdpchild.task instanceof ToIntFunction);
		assertTrue(mdpchild.task == tointfunction);
	}

	@Test
	public void testMassiveDataPipelineMaptoIntFormDAGAbsFunc() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		SToIntFunction<String> tointfunction = (dat) -> Integer.parseInt(dat);
		PipelineIntStream<String> mdpchild = mdp.mapToInt(tointfunction);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineKeyBy() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		KeyByFunction<String, String> keybyfunction = (dat) -> dat;
		MapPair<String, String> mdpchild = mdp.keyBy(keybyfunction);
		assertTrue(mdpchild.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == mdpchild);
		assertTrue(mdpchild.task instanceof KeyByFunction);
		assertTrue(mdpchild.task == keybyfunction);
	}

	@Test
	public void testMassiveDataPipelineKeyByFormDAGAbsFunc() throws Exception {
		MassiveDataPipeline<String> mdp = new MassiveDataPipeline<String>();
		KeyByFunction<String, String> keybyfunction = (dat) -> dat;
		MapPair<String, String> mdpchild = mdp.keyBy(keybyfunction);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetDAGMap() throws Exception {
		String config = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MassiveDataPipeline<String> filter = mdp.map(dat -> dat).filter(dat -> dat.equals(dat));
		MapPair<String, Long> mappair = filter.mapToPair(dat -> new Tuple2<String, Long>(dat, Long.parseLong(dat)));
		MapPair<String, Long> redbykeyleft = mappair.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);
		MapPair<String, Long> redbykeyright = mappair.reduceByKey((dat1, dat2) -> dat1 - dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);
		MassiveDataPipeline<String> mdproot = (MassiveDataPipeline<String>) redbykeyleft.join(redbykeyright,
				(left, right) -> left.equals(right)).root;
		mdproot.finaltasks.add(mdproot.finaltask);
		Job job = new Job();
		job.jm = new JobMetrics();
		mdproot.getDAG(job);
		pipelineconfig.setBlocksize(config);

		assertEquals(9, mdproot.graph.vertexSet().size());
		assertEquals(9, mdproot.graph.edgeSet().size());
	}
}