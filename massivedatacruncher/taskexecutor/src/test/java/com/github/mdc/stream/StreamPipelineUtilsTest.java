package com.github.mdc.stream;

import java.util.Arrays;
import java.util.function.IntUnaryOperator;
import java.util.function.ToIntFunction;

import org.jooq.lambda.tuple.Tuple2;
import org.junit.Test;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.stream.CsvOptions;
import com.github.mdc.stream.PipelineUtils;
import com.github.mdc.stream.PipelineIntStreamCollect;
import com.github.mdc.stream.SampleSupplierInteger;
import com.github.mdc.stream.SampleSupplierPartition;
import com.github.mdc.stream.functions.AggregateFunction;
import com.github.mdc.stream.functions.AggregateReduceFunction;
import com.github.mdc.stream.functions.CalculateCount;
import com.github.mdc.stream.functions.CoalesceFunction;
import com.github.mdc.stream.functions.CountByKeyFunction;
import com.github.mdc.stream.functions.CountByValueFunction;
import com.github.mdc.stream.functions.DoubleFlatMapFunction;
import com.github.mdc.stream.functions.FlatMapFunction;
import com.github.mdc.stream.functions.FoldByKey;
import com.github.mdc.stream.functions.GroupByKeyFunction;
import com.github.mdc.stream.functions.IntersectionFunction;
import com.github.mdc.stream.functions.JoinPredicate;
import com.github.mdc.stream.functions.LeftOuterJoinPredicate;
import com.github.mdc.stream.functions.LongFlatMapFunction;
import com.github.mdc.stream.functions.MapFunction;
import com.github.mdc.stream.functions.MapToPairFunction;
import com.github.mdc.stream.functions.PeekConsumer;
import com.github.mdc.stream.functions.PredicateSerializable;
import com.github.mdc.stream.functions.ReduceByKeyFunction;
import com.github.mdc.stream.functions.RightOuterJoinPredicate;
import com.github.mdc.stream.functions.SortedComparator;
import com.github.mdc.stream.functions.TupleFlatMapFunction;
import com.github.mdc.stream.functions.UnionFunction;

import junit.framework.TestCase;

public class StreamPipelineUtilsTest extends TestCase{
	
	@Test
	public void testPredicateSerializable() {
		
		PredicateSerializable<String> predicateSerializable = (value)->value.equals("value");
		Object task = predicateSerializable;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.PREDICATESERIALIZABLE,printableTask);
	}
	
	@Test
	public void testMapPairFunction() {
		
		MapToPairFunction<String,Tuple2<String,String>> mapPairFunction = (value)->new Tuple2<String,String>(value,value);
		Object task = mapPairFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.MAPPAIRFUNCTION,printableTask);
	}
	
	
	@Test
	public void testMapFunction() {
		MapFunction<String,String> mapFunction = (value)->value+"100";
		Object task = mapFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.MAPFUNCTION,printableTask);
	}
	
	@Test
	public void testReduceFunction() {
		
		ReduceByKeyFunction<String> reduceFunction = (value1,value2)->value1+" "+value2;
		Object task = reduceFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.REDUCEFUNCTION,printableTask);
	}
	
	@Test
	public void testFlatMapFunction() {
		
		FlatMapFunction<String,String> flatMapFunction = (val)->Arrays.asList(val);
		Object task = flatMapFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.FLATMAPFUNCTION,printableTask);
	}
	
	
	@Test
	public void testJoinTuplePredicate() {
		
		JoinPredicate<String,String> joinPredicate = (val1,val2)->val1.equals(val2);
		Object task = joinPredicate;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.JOINTUPLEPREDICATE,printableTask);
	}
	
	@Test
	public void testLeftOuterJoinPredicate() {
		
		LeftOuterJoinPredicate<String,String> joinPredicate = (val1,val2)->val1.equals(val2);
		Object task = joinPredicate;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.LEFTOUTERJOINTUPLEPREDICATE,printableTask);
	}
	
	
	@Test
	public void testRightOuterJoinPredicate() {
		
		RightOuterJoinPredicate<String,String> joinPredicate = (val1,val2)->val1.equals(val2);
		Object task = joinPredicate;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.RIGHTOUTERJOINTUPLEPREDICATE,printableTask);
	}
	
	
	@Test
	public void testGroupByKeyFunction() {
		
		GroupByKeyFunction groupByKeyFunction = new GroupByKeyFunction();
		Object task = groupByKeyFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.GROUPBYKEYFUNCTION,printableTask);
	}
	
	
	@Test
	public void testAggregateReduceFunction() {
		
		AggregateReduceFunction<String,String,String> aggregateReduceFunction = (val1,val2)->val1+val2;
		Object task = aggregateReduceFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.AGGREGATEREDUCEFUNCTION,printableTask);
	}
	
	
	@Test
	public void testAggregateFunction() {
		
		AggregateFunction<String,String,String> aggregateFunction = (val1,val2)->val1+val2;
		Object task = aggregateFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.AGGREGATEFUNCTION,printableTask);
	}
	
	@Test
	public void testSampleSupplierInteger() {
		
		SampleSupplierInteger sampleSupplier = new SampleSupplierInteger(100);
		Object task = sampleSupplier;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.SAMPLESUPPLIERINTEGER,printableTask);
	}
	
	@Test
	public void testSampleSupplierPartition() {
		
		SampleSupplierPartition sampleSupplier = new SampleSupplierPartition(100);
		Object task = sampleSupplier;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.SAMPLESUPPLIERPARTITION,printableTask);
	}
	
	
	@Test
	public void testUnionFunction() {
		
		UnionFunction unionFunction = new UnionFunction();
		Object task = unionFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.UNIONFUNCTION,printableTask);
	}
	
	@Test
	public void testIntersectionFunction() {
		
		IntersectionFunction intersectionFunction = new IntersectionFunction();
		Object task = intersectionFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.INTERSECTIONFUNCTION,printableTask);
	}
	
	@Test
	public void testTupleFlatMapFunction() {
		
		TupleFlatMapFunction<String,Tuple2<String,String>> tupleFlatMapFunction = (val)->Arrays.asList(new Tuple2<String,String>(val,val));
		Object task = tupleFlatMapFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.PAIRFLATMAPFUNCTION,printableTask);
	}
	
	
	@Test
	public void testLongFlatMapFunction() {
		
		LongFlatMapFunction<String> longFlatMapFunction = (val)->Arrays.asList(Long.parseLong(val));
		Object task = longFlatMapFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.LONGFLATMAPFUNCTION,printableTask);
	}
	
	@Test
	public void testDoubleFlatMapFunction() {
		
		DoubleFlatMapFunction<String> doubleFlatMapFunction = (val)->Arrays.asList(Double.parseDouble(val));
		Object task = doubleFlatMapFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.DOUBLEFLATMAPFUNCTION,printableTask);
	}
	
	
	@Test
	public void testCoalesceFunction() {
		
		CoalesceFunction<String> coalesceFunction = (val1,val2)->val1+val2;
		Object task = coalesceFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.COALESCEFUNCTION,printableTask);
	}
	
	@Test
	public void testCsvOptions() {
		
		CsvOptions csvOptions = new CsvOptions(new String[] {"Month","DayOfMonth"});
		Object task = csvOptions;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.CSVOPTIONS,printableTask);
	}
	
	
	@Test
	public void testPeekConsumer() {
		
		PeekConsumer<String> peekConsumer = System.out::println;
		Object task = peekConsumer;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.PEEKCONSUMER,printableTask);
	}
	
	
	@Test
	public void testSorted() {
		
		SortedComparator<String> sorted = (Val1,val2)->Val1.compareTo(val2);
		Object task = sorted;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.SORTEDCOMPARATOR,printableTask);
	}
	
	
	@Test
	public void testCalculateCount() {
		
		CalculateCount calculateCount = new CalculateCount();
		Object task = calculateCount;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.CALCULATECOUNT,printableTask);
	}
	
	@Test
	public void testMapToInt() {
		
		ToIntFunction<String> toIntFunction = (val)->Integer.parseInt(val);
		Object task = toIntFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.MAPTOINT,printableTask);
	}
	
	
	@Test
	public void testPipelineIntStreamCollect() {
				
		PipelineIntStreamCollect pipelineIntStreamCollect = new PipelineIntStreamCollect(null,null,null);
		Object task = pipelineIntStreamCollect;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.PIPELINEINTSTREAMCOLLECT,printableTask);
	}
	
	
	@Test
	public void testIntUnaryOperator() {
				
		IntUnaryOperator intUnaryOperator = (val)->val++;
		Object task = intUnaryOperator;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.INTUNARYOPERATOR,printableTask);
	}
	
	
	@Test
	public void testCountByKeyFunction() {
				
		CountByKeyFunction countByKeyFunction = new CountByKeyFunction();
		Object task = countByKeyFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.COUNTBYKEYFUNCTION,printableTask);
	}
	
	@Test
	public void testCountByValueFunction() {
				
		CountByValueFunction countByValueFunction = new CountByValueFunction();
		Object task = countByValueFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.COUNTBYVALUEFUNCTION,printableTask);
	}
	
	
	@Test
	public void testFoldByKeyFunction() {
				
		FoldByKey foldByKeyFunction = new FoldByKey(100l,(val1,val2)->val1, true);
		Object task = foldByKeyFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(MDCConstants.FOLDBYKEY,printableTask);
	}
}
