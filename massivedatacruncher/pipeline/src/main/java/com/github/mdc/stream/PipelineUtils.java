package com.github.mdc.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.function.ToIntFunction;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.Task;
import com.github.mdc.stream.functions.AggregateFunction;
import com.github.mdc.stream.functions.AggregateReduceFunction;
import com.github.mdc.stream.functions.CalculateCount;
import com.github.mdc.stream.functions.Coalesce;
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
import com.github.mdc.stream.functions.MapValuesFunction;
import com.github.mdc.stream.functions.Max;
import com.github.mdc.stream.functions.Min;
import com.github.mdc.stream.functions.PeekConsumer;
import com.github.mdc.stream.functions.PredicateSerializable;
import com.github.mdc.stream.functions.ReduceByKeyFunction;
import com.github.mdc.stream.functions.ReduceByKeyFunctionValues;
import com.github.mdc.stream.functions.RightOuterJoinPredicate;
import com.github.mdc.stream.functions.SortedComparator;
import com.github.mdc.stream.functions.StandardDeviation;
import com.github.mdc.stream.functions.Sum;
import com.github.mdc.stream.functions.SummaryStatistics;
import com.github.mdc.stream.functions.TupleFlatMapFunction;
import com.github.mdc.stream.functions.UnionFunction;

/**
 * 
 * @author arun
 * The class MassiveDataPipelineUtils is helper class for Pipeline.
 */
public class PipelineUtils {
	private PipelineUtils() {
	}

	/**
	 * The function returns the function name based on the type of 
	 * function provided as parameter to the getFunctions. 
	 * @param task
	 * @return function name.
	 */
	public static String getFunctions(Object obj) {
		if (obj instanceof PredicateSerializable) {
			return MDCConstants.PREDICATESERIALIZABLE;
		} else if (obj instanceof MapToPairFunction) {
			return MDCConstants.MAPPAIRFUNCTION;
		} else if (obj instanceof MapFunction) {
			return MDCConstants.MAPFUNCTION;
		} else if (obj instanceof ReduceByKeyFunction) {
			return MDCConstants.REDUCEFUNCTION;
		} else if (obj instanceof FlatMapFunction) {
			return MDCConstants.FLATMAPFUNCTION;
		} else if (obj instanceof JoinPredicate) {
			return MDCConstants.JOINTUPLEPREDICATE;
		} else if (obj instanceof LeftOuterJoinPredicate) {
			return MDCConstants.LEFTOUTERJOINTUPLEPREDICATE;
		} else if (obj instanceof RightOuterJoinPredicate) {
			return MDCConstants.RIGHTOUTERJOINTUPLEPREDICATE;
		} else if (obj instanceof GroupByKeyFunction) {
			return MDCConstants.GROUPBYKEYFUNCTION;
		} else if (obj instanceof AggregateReduceFunction) {
			return MDCConstants.AGGREGATEREDUCEFUNCTION;
		} else if (obj instanceof AggregateFunction) {
			return MDCConstants.AGGREGATEFUNCTION;
		} else if (obj instanceof SampleSupplierInteger) {
			return MDCConstants.SAMPLESUPPLIERINTEGER;
		} else if (obj instanceof SampleSupplierPartition) {
			return MDCConstants.SAMPLESUPPLIERPARTITION;
		} else if (obj instanceof UnionFunction) {
			return MDCConstants.UNIONFUNCTION;
		} else if (obj instanceof IntersectionFunction) {
			return MDCConstants.INTERSECTIONFUNCTION;
		} else if (obj instanceof TupleFlatMapFunction) {
			return MDCConstants.PAIRFLATMAPFUNCTION;
		} else if (obj instanceof LongFlatMapFunction) {
			return MDCConstants.LONGFLATMAPFUNCTION;
		} else if (obj instanceof DoubleFlatMapFunction) {
			return MDCConstants.DOUBLEFLATMAPFUNCTION;
		} else if (obj instanceof CoalesceFunction) {
			return MDCConstants.COALESCEFUNCTION;
		} else if (obj instanceof CsvOptions) {
			return MDCConstants.CSVOPTIONS;
		} else if (obj instanceof PeekConsumer) {
			return MDCConstants.PEEKCONSUMER;
		} else if (obj instanceof SortedComparator) {
			return MDCConstants.SORTEDCOMPARATOR;
		} else if (obj instanceof CalculateCount) {
			return MDCConstants.CALCULATECOUNT;
		} else if (obj instanceof ToIntFunction) {
			return MDCConstants.MAPTOINT;
		} else if (obj instanceof PipelineIntStreamCollect) {
			return MDCConstants.PIPELINEINTSTREAMCOLLECT;
		} else if (obj instanceof IntUnaryOperator) {
			return MDCConstants.INTUNARYOPERATOR;
		} else if (obj instanceof CountByKeyFunction) {
			return MDCConstants.COUNTBYKEYFUNCTION;
		} else if (obj instanceof CountByValueFunction) {
			return MDCConstants.COUNTBYVALUEFUNCTION;
		} else if (obj instanceof FoldByKey) {
			return MDCConstants.FOLDBYKEY;
		} else if (obj instanceof Json) {
			return MDCConstants.JSON;
		} else if (obj instanceof Coalesce) {
			return MDCConstants.COALESCE;
		} else if (obj instanceof SummaryStatistics) {
			return MDCConstants.SUMMARYSTATISTICS;
		} else if (obj instanceof Sum) {
			return MDCConstants.SUM;
		} else if (obj instanceof Max) {
			return MDCConstants.MAX;
		} else if (obj instanceof Min) {
			return MDCConstants.MIN;
		} else if (obj instanceof StandardDeviation) {
			return MDCConstants.STANDARDDEVIATION;
		} else if (obj instanceof MapValuesFunction) {
			return MDCConstants.MAPVALUESFUNCTION;
		} else if (obj instanceof ReduceByKeyFunctionValues) {
			return MDCConstants.REDUCEFUNCTIONVALUES;
		}
		return MDCConstants.EMPTY;
	}

	/**
	 * The function returns the list of function name in list.
	 * @param tasks
	 * @return list of functions name.
	 */
	public static List<String> getFunctions(List<Task> tasks) {
		var functions = new ArrayList<String>();
		for (var task : tasks) {
			functions.add(PipelineUtils.getFunctions(task));
		}
		return functions;
	}
}
