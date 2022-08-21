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

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.function.ToIntFunction;

import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.Task;
import com.github.mdc.common.functions.AggregateFunction;
import com.github.mdc.common.functions.AggregateReduceFunction;
import com.github.mdc.common.functions.CalculateCount;
import com.github.mdc.common.functions.Coalesce;
import com.github.mdc.common.functions.CoalesceFunction;
import com.github.mdc.common.functions.CountByKeyFunction;
import com.github.mdc.common.functions.CountByValueFunction;
import com.github.mdc.common.functions.DoubleFlatMapFunction;
import com.github.mdc.common.functions.FlatMapFunction;
import com.github.mdc.common.functions.FoldByKey;
import com.github.mdc.common.functions.GroupByKeyFunction;
import com.github.mdc.common.functions.IntersectionFunction;
import com.github.mdc.common.functions.JoinPredicate;
import com.github.mdc.common.functions.LeftOuterJoinPredicate;
import com.github.mdc.common.functions.LongFlatMapFunction;
import com.github.mdc.common.functions.MapFunction;
import com.github.mdc.common.functions.MapToPairFunction;
import com.github.mdc.common.functions.MapValuesFunction;
import com.github.mdc.common.functions.Max;
import com.github.mdc.common.functions.Min;
import com.github.mdc.common.functions.PeekConsumer;
import com.github.mdc.common.functions.PredicateSerializable;
import com.github.mdc.common.functions.ReduceByKeyFunction;
import com.github.mdc.common.functions.ReduceByKeyFunctionValues;
import com.github.mdc.common.functions.RightOuterJoinPredicate;
import com.github.mdc.common.functions.SortedComparator;
import com.github.mdc.common.functions.StandardDeviation;
import com.github.mdc.common.functions.Sum;
import com.github.mdc.common.functions.SummaryStatistics;
import com.github.mdc.common.functions.TupleFlatMapFunction;
import com.github.mdc.common.functions.UnionFunction;

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
