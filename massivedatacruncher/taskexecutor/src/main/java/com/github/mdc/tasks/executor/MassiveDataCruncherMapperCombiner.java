package com.github.mdc.tasks.executor;

import java.util.List;

import org.xerial.snappy.SnappyInputStream;

import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.Context;

public class MassiveDataCruncherMapperCombiner extends MassiveDataCruncherMapper{

	BlocksLocation blockslocation;
	@SuppressWarnings("rawtypes")
	List<CrunchMapper> crunchmappers;
	@SuppressWarnings("rawtypes")
	List<CrunchCombiner> crunchcombiners;
	@SuppressWarnings("rawtypes")
	public MassiveDataCruncherMapperCombiner(BlocksLocation blockslocation,SnappyInputStream datastream,List<CrunchMapper> crunchmappers,
			List<CrunchCombiner> crunchcombiners) {
		super(blockslocation,datastream,crunchmappers);
		this.crunchcombiners = crunchcombiners;
	}
	
	@SuppressWarnings({"rawtypes" })
	@Override
	public Context call() throws Exception {
		var starttime = System.currentTimeMillis();
		var ctx = super.call();
		if(crunchcombiners!=null && crunchcombiners.size()>0) {
			var mdcc = new MassiveDataCruncherCombiner(ctx,crunchcombiners.get(0));
			ctx = mdcc.call();
			return ctx;
		}
		var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
		log.info("Time taken to compute mapper Task is " + timetaken + " seconds");
		return ctx;
	}

}
