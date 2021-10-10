package com.github.mdc.tasks.executor;

import java.util.List;

import org.xerial.snappy.SnappyInputStream;

import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.Context;

public class MapperCombinerExecutor extends MapperExecutor{

	BlocksLocation blockslocation;
	@SuppressWarnings("rawtypes")
	List<Mapper> crunchmappers;
	@SuppressWarnings("rawtypes")
	List<Combiner> crunchcombiners;
	@SuppressWarnings("rawtypes")
	public MapperCombinerExecutor(BlocksLocation blockslocation,SnappyInputStream datastream,List<Mapper> crunchmappers,
			List<Combiner> crunchcombiners) {
		super(blockslocation,datastream,crunchmappers);
		this.crunchcombiners = crunchcombiners;
	}
	
	@SuppressWarnings({"rawtypes" })
	@Override
	public Context call() throws Exception {
		var starttime = System.currentTimeMillis();
		var ctx = super.call();
		if(crunchcombiners!=null && crunchcombiners.size()>0) {
			var mdcc = new CombinerExecutor(ctx,crunchcombiners.get(0));
			ctx = mdcc.call();
			return ctx;
		}
		var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
		log.info("Time taken to compute mapper Task is " + timetaken + " seconds");
		return ctx;
	}

}
