package com.github.mdc.common;

import org.jgrapht.graph.DefaultWeightedEdge;

/**
 * 
 * @author Arun
 * The jgrapht edge implementation with custom source and target object. 
 */
public class DAGEdge extends DefaultWeightedEdge {

	private static final long serialVersionUID = 2853307464354054088L;

	public Object getSource()
    {
        return super.getSource();
    } 
	public Object getTarget()
    {
        return super.getTarget();
    }
}
