package com.github.mdc.stream;

import org.jooq.lambda.tuple.Tuple2;

import com.github.mdc.stream.functions.MapValuesFunction;
import com.github.mdc.stream.functions.ReduceByKeyFunctionValues;

/**
 * 
 * @author arun
 * This class holds information for MapValues function.
 * @param <I1>
 * @param <I2>
 */
public final class MapValues<I1,I2> extends MapPair<I1,I2> {
	@SuppressWarnings({ "unchecked" })
	private MapValues(AbstractPipeline root,
			ReduceByKeyFunctionValues<I2> rfv)  {
		this.task = rfv;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * Constructor for MapValues
	 * @param <I3>
	 * @param <I4>
	 * @param root
	 * @param mvf
	 */
	@SuppressWarnings("unchecked")
	public <I3,I4> MapValues(AbstractPipeline root, MapValuesFunction<? super I2, ? extends Tuple2<I3,I4>> mvf) {
		this.task = mvf;
		this.root = root;
		root.finaltask=task;
	}
	
	/**
	 * MapValues accepts ReduceFunctionValues.
	 * @param rfv
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public MapValues<I1,I2> reduceByValues(ReduceByKeyFunctionValues<I2> rfv)  {
		var mapvalues = new MapValues(root, rfv);
		this.childs.add(mapvalues);
		mapvalues.parents.add(this);
		return mapvalues;
	}
}
