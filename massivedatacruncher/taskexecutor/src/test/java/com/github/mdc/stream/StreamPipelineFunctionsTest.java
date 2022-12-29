/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.mdc.stream;

import java.util.Arrays;
import java.util.List;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Test;
import com.github.mdc.common.functions.AggregateFunction;
import com.github.mdc.common.functions.AggregateReduceFunction;
import com.github.mdc.common.functions.BiPredicateSerializable;
import com.github.mdc.common.functions.CoalesceFunction;
import com.github.mdc.common.functions.DoubleFlatMapFunction;
import com.github.mdc.common.functions.FlatMapFunction;
import com.github.mdc.common.functions.JoinPredicate;
import com.github.mdc.common.functions.KeyByFunction;
import com.github.mdc.common.functions.LeftOuterJoinPredicate;
import com.github.mdc.common.functions.LongFlatMapFunction;
import com.github.mdc.common.functions.MapFunction;
import com.github.mdc.common.functions.MapToPairFunction;
import com.github.mdc.common.functions.PairFunction;
import com.github.mdc.common.functions.PeekConsumer;
import com.github.mdc.common.functions.PredicateSerializable;
import com.github.mdc.common.functions.ReduceByKeyFunction;
import com.github.mdc.common.functions.RightOuterJoinPredicate;
import com.github.mdc.common.functions.TupleFlatMapFunction;
import junit.framework.TestCase;

public class StreamPipelineFunctionsTest extends TestCase {
	@Test
	public void testAggregateFunction() {
		AggregateFunction<String, String, String> aggregateFunction = (a, b) -> a + b;
		String result = aggregateFunction.apply("MDC", "-MassiveDataCruncher");
		assertEquals("MDC-MassiveDataCruncher", result);
		AggregateFunction<Long, Long, Long> aggregateFunctionL = (a, b) -> a + b;
		Long resultL = aggregateFunctionL.apply(10l, 20l);
		assertEquals(30l, resultL.longValue());
		AggregateFunction<Long, String, String> aggregateFunctionSL = (a, b) -> a + b;
		String resultSL = aggregateFunctionSL.apply(10l, "20");
		assertEquals("1020", resultSL);

	}

	@Test
	public void testAggregateReduceFunction() {
		AggregateReduceFunction<String, String, String> aggregateReduceFunction = (a, b) -> a + b;
		String result = aggregateReduceFunction.apply("MDC", "-MassiveDataCruncher");
		assertEquals("MDC-MassiveDataCruncher", result);
		AggregateReduceFunction<Long, Long, Long> aggregateReduceFunctionL = (a, b) -> a + b;
		Long resultL = aggregateReduceFunctionL.apply(10l, 20l);
		assertEquals(30l, resultL.longValue());
		AggregateReduceFunction<Long, String, String> aggregateReduceFunctionSL = (a, b) -> a + b;
		String resultSL = aggregateReduceFunctionSL.apply(10l, "20");
		assertEquals("1020", resultSL);

	}

	@Test
	public void testBiPredicateSerializable() {
		BiPredicateSerializable<String, String> biPredicateSerializable = (a, b) -> a.equals(b);
		boolean result = biPredicateSerializable.test("MDC", "MDC");
		assertTrue(result);
		result = biPredicateSerializable.test("MDC", "MDCTest");
		assertFalse(result);
		result = biPredicateSerializable.negate().test("MDC", "MDCTest");
		assertTrue(result);
	}

	@Test
	public void testCoalesceFunction() {
		CoalesceFunction<String> coalesceFunctionS = (a, b) -> a + b;
		String resultS = coalesceFunctionS.apply("MDC", "MDC");
		assertEquals("MDCMDC", resultS);
		CoalesceFunction<Long> coalesceFunctionL = (a, b) -> a + b;
		long resultL = coalesceFunctionL.apply(20l, 30l);
		assertEquals(50, resultL);
	}

	@Test
	public void testDoubleFlatMapFunction() {
		DoubleFlatMapFunction<String> doubleFlatMapFunctionSD = a -> Arrays.asList(Double.parseDouble(a));
		List<Double> resultSD = doubleFlatMapFunctionSD.apply("100");
		assertEquals(100.0, resultSD.get(0));
		DoubleFlatMapFunction<Double> doubleFlatMapFunctionDD = a -> Arrays.asList(a);
		List<Double> resultDD = doubleFlatMapFunctionDD.apply(100d);
		assertEquals(100.0d, resultDD.get(0));
	}

	@Test
	public void testFlatMapFunction() {
		FlatMapFunction<String, String> flatMapFunctionSS = a -> Arrays.asList(a);
		List<String> resultSS = flatMapFunctionSS.apply("100");
		assertEquals("100", resultSS.get(0));
		FlatMapFunction<Double, Double> flatMapFunctionDD = a -> Arrays.asList(a);
		List<Double> resultDD = flatMapFunctionDD.apply(100d);
		assertEquals(100.0d, resultDD.get(0));
	}

	@Test
	public void testJoinPredicate() {
		JoinPredicate<String, String> joinPredicateSS = (a, b) -> a.equals(b);
		boolean result = joinPredicateSS.test("MDC", "MDC");
		assertTrue(result);
		result = joinPredicateSS.test("MDC", "MDCTest");
		assertFalse(result);
		result = joinPredicateSS.negate().test("MDC", "MDCTest");
		assertTrue(result);
		JoinPredicate<Long, Long> joinPredicateLL = (a, b) -> a.equals(b);
		result = joinPredicateLL.test(100l, 100l);
		assertTrue(result);
		result = joinPredicateLL.test(100l, 101l);
		assertFalse(result);
		result = joinPredicateLL.negate().test(100l, 101l);
		assertTrue(result);
	}

	@Test
	public void testKeyByFunction() {
		KeyByFunction<String, String> keyByFunctionSS = a -> a + "-MassiveDataCruncher";
		String resultSS = keyByFunctionSS.apply("MDC");
		assertEquals("MDC-MassiveDataCruncher", resultSS);
		KeyByFunction<Double, Double> keyByFunctionDD = a -> a + 100;
		Double resultDD = keyByFunctionDD.apply(100.0d);
		assertEquals(200.0, resultDD);
	}

	@Test
	public void testLeftOuterJoinPredicate() {
		LeftOuterJoinPredicate<String, String> leftOuterJoinPredicateSB = (a, b) -> a.equals(b);
		boolean resultSB = leftOuterJoinPredicateSB.test("MDC", "MDC");
		assertTrue(resultSB);
		leftOuterJoinPredicateSB = (a, b) -> a.equals(b);
		resultSB = leftOuterJoinPredicateSB.test("MDC", "-MassiveDataCruncher");
		assertFalse(resultSB);
		LeftOuterJoinPredicate<Double, Double> leftOuterJoinPredicateDB = (a, b) -> a.equals(b);
		boolean resultDB = leftOuterJoinPredicateDB.test(100d, 100d);
		assertTrue(resultDB);
		LeftOuterJoinPredicate<String, Double> leftOuterJoinPredicateSDB = (a, b) -> a.equals(b);
		boolean resultSDB = leftOuterJoinPredicateSDB.test("100", 100d);
		assertFalse(resultSDB);
		resultSDB = leftOuterJoinPredicateSDB.negate().test("100", 100d);
		assertTrue(resultSDB);
	}


	@Test
	public void testLongFlatMapFunction() {
		LongFlatMapFunction<String> longFlatMapFunctionSL = a -> Arrays.asList(Long.parseLong(a));
		List<Long> resultSL = longFlatMapFunctionSL.apply("100");
		assertEquals(100l, resultSL.get(0).longValue());
		LongFlatMapFunction<Long> longFlatMapFunctionLL = a -> Arrays.asList(a);
		List<Long> resultLL = longFlatMapFunctionLL.apply(100l);
		assertEquals(100l, resultLL.get(0).longValue());
	}

	@Test
	public void testMapFunction() {
		MapFunction<String, String> mapFunction = a -> a + "100";
		String result = mapFunction.apply("MDC");
		assertEquals("MDC100", result);
		MapFunction<Long, Long> mapFunctionLL = a -> a + 100;
		Long resultLL = mapFunctionLL.apply(10l);
		assertEquals(110l, resultLL.longValue());
		MapFunction<Long, String> mapFunctionLS = a -> a + "100";
		String resultLS = mapFunctionLS.apply(10l);
		assertEquals("10100", resultLS);
		MapFunction<String, Long> mapFunctionSL = a -> Long.parseLong(a);
		Long resultSL = mapFunctionSL.apply("100");
		assertEquals(100l, resultSL.longValue());

	}

	@Test
	public void testPairFunction() {
		PairFunction<String, String> pairFunction = a -> a + "100";
		String result = pairFunction.apply("MDC");
		assertEquals("MDC100", result);
		PairFunction<Long, Long> pairFunctionLL = a -> a + 100;
		Long resultLL = pairFunctionLL.apply(10l);
		assertEquals(110l, resultLL.longValue());
		PairFunction<Long, String> pairFunctionLS = a -> a + "100";
		String resultLS = pairFunctionLS.apply(10l);
		assertEquals("10100", resultLS);
		PairFunction<String, Long> pairFunctionSL = a -> Long.parseLong(a);
		Long resultSL = pairFunctionSL.apply("100");
		assertEquals(100l, resultSL.longValue());

	}


	@SuppressWarnings("unchecked")
	@Test
	public void testMapPairFunction() {
		MapToPairFunction<String, Tuple> mapPairFunction = a -> new Tuple2<String, String>(a, a);
		Tuple2<String, String> result = (Tuple2<String, String>) mapPairFunction.apply("MDC");
		assertEquals("MDC", result.v1);
		assertEquals("MDC", result.v2);

		MapToPairFunction<Long, Tuple> mapPairFunctionLT = a -> new Tuple2<Long, Long>(a, a);
		Tuple2<Long, Long> resultLT = (Tuple2<Long, Long>) mapPairFunctionLT.apply(100l);
		assertEquals(100l, resultLT.v1.longValue());
		assertEquals(100l, resultLT.v2.longValue());

	}
	String dataS;
	Long valL;

	@Test
	public void testPeekConsumer() {
		PeekConsumer<String> consumerS = data1 -> dataS = data1;
		consumerS.accept("100");
		assertEquals("100", dataS);
		PeekConsumer<Long> consumerL = data1 -> valL = data1;
		consumerL.accept(100l);
		assertEquals(100, valL.longValue());
	}

	@Test
	public void testPredicateSerializable() {
		PredicateSerializable<String> predicateSerializableSS = a -> "MDC".equals(a);
		boolean result = predicateSerializableSS.test("MDC");
		assertTrue(result);
		result = predicateSerializableSS.negate().test("MDC");
		assertFalse(result);
		PredicateSerializable<Long> joinPredicateLL = a -> a.equals(100l);
		result = joinPredicateLL.test(100l);
		assertTrue(result);
		result = joinPredicateLL.negate().test(100l);
		assertFalse(result);
	}

	@Test
	public void testReduceFunction() {
		ReduceByKeyFunction<String> reduceFunctionSS = (a, b) -> a + b;
		String result = reduceFunctionSS.apply("MDC", "-MassiveDataCruncher");
		assertEquals("MDC-MassiveDataCruncher", result);
		ReduceByKeyFunction<Long> reduceFunctionLL = (a, b) -> a + b;
		Long resultL = reduceFunctionLL.apply(100l, 100l);
		assertEquals(200l, resultL.longValue());
		ReduceByKeyFunction<Double> reduceFunctionDD = (a, b) -> a + b;
		Double resultD = reduceFunctionDD.apply(100.0, 100.0);
		assertEquals(200.0, resultD.doubleValue());
	}

	@Test
	public void testRightOuterJoinPredicate() {
		RightOuterJoinPredicate<String, String> rightOuterJoinPredicateSB = (a, b) -> a.equals(b);
		boolean resultSB = rightOuterJoinPredicateSB.test("MDC", "MDC");
		assertTrue(resultSB);
		rightOuterJoinPredicateSB = (a, b) -> a.equals(b);
		resultSB = rightOuterJoinPredicateSB.test("MDC", "-MassiveDataCruncher");
		assertFalse(resultSB);
		RightOuterJoinPredicate<Double, Double> rightOuterJoinPredicateDB = (a, b) -> a.equals(b);
		boolean resultDB = rightOuterJoinPredicateDB.test(100d, 100d);
		assertTrue(resultDB);
		RightOuterJoinPredicate<String, Double> rightOuterJoinPredicateSDB = (a, b) -> a.equals(b);
		boolean resultSDB = rightOuterJoinPredicateSDB.test("100", 100d);
		assertFalse(resultSDB);
		resultSDB = rightOuterJoinPredicateSDB.negate().test("100", 100d);
		assertTrue(resultSDB);
	}

	@Test
	public void testTupleFlatMapFunction() {
		TupleFlatMapFunction<String, Tuple2<String, String>> tupleFlatMapFunction = data -> Arrays.asList(new Tuple2<String, String>(data, data + "100"));
		List<Tuple2<String, String>> tuples = tupleFlatMapFunction.apply("data");
		assertEquals("data", tuples.get(0).v1);
		assertEquals("data100", tuples.get(0).v2);
		TupleFlatMapFunction<Long, Tuple2<Long, Long>> tupleFlatMapFunctionL = data -> Arrays.asList(new Tuple2<Long, Long>(data, data + 100));
		List<Tuple2<Long, Long>> tuplesL = tupleFlatMapFunctionL.apply(100l);
		assertEquals(100l, tuplesL.get(0).v1.longValue());
		assertEquals(200l, tuplesL.get(0).v2.longValue());
	}
}
