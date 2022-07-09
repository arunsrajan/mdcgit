package com.github.mdc.stream.functions;

import java.io.Serializable;

public class Coalesce<I1> implements Serializable {

	private static final long serialVersionUID = 458889144182953938L;

	public Coalesce() {
		super();
	}
	public int coalescepartition;
	public CoalesceFunction<I1> coalescefuncion;

	public Coalesce(int coalescepartition, CoalesceFunction<I1> coalescefuncion) {
		super();
		this.coalescepartition = coalescepartition;
		this.coalescefuncion = coalescefuncion;
	}

}
