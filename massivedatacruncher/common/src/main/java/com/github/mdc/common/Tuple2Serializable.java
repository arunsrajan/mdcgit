package com.github.mdc.common;

import java.io.Serializable;

import org.jooq.lambda.tuple.Tuple2;

/**
 * 
 * @author arun
 * This class is the derivied class of the Tuple2 with serializable.
 * @param <V1>
 * @param <V2>
 */
public class Tuple2Serializable<V1, V2> extends Tuple2<V1, V2> implements Serializable {

	private static final long serialVersionUID = -622259929441801409L;

	
	
	public Tuple2Serializable() {
		super(null,null);
	}



	public Tuple2Serializable(V1 v1, V2 v2) {
		super(v1, v2);
	}

}
