package com.github.mdc.tasks.scheduler.ignite;

import java.util.List;

import org.apache.ignite.lang.IgniteCallable;
import org.jgroups.util.UUID;

import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.tasks.executor.CrunchCombiner;
import com.github.mdc.tasks.executor.CrunchMapper;

public class MassiveDataCruncherMapperCombiner extends MassiveDataCruncherMapper implements IgniteCallable<MapReduceResult>{
	private static final long serialVersionUID = -4560060224786371070L;
	@SuppressWarnings("rawtypes")
	List<CrunchMapper> crunchmappers;
	@SuppressWarnings("rawtypes")
	List<CrunchCombiner> crunchcombiners;
	@SuppressWarnings("rawtypes")
	public MassiveDataCruncherMapperCombiner(BlocksLocation blockslocation,List<CrunchMapper> crunchmappers,
			List<CrunchCombiner> crunchcombiners) {
		super(blockslocation,crunchmappers);
		this.crunchcombiners = crunchcombiners;
	}
	
	@SuppressWarnings({"rawtypes" })
	@Override
	public MapReduceResult call() throws Exception {
		var starttime = System.currentTimeMillis();
		var ctx = super.execute();
		if(crunchcombiners!=null && crunchcombiners.size()>0) {
			var mdcc = new MassiveDataCruncherCombiner(ctx,crunchcombiners.get(0));
			ctx = mdcc.call();
		}
		var mrresult = new MapReduceResult();
		mrresult.cachekey = UUID.randomUUID().toString();
		var cache = ignite.getOrCreateCache(MDCConstants.MDCCACHEMR);
		cache.put(mrresult.cachekey, (DataCruncherContext) ctx);
		var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
		log.info("Time taken to compute mapper Task is " + timetaken + " seconds");
		return mrresult;
	}

	public BlocksLocation getBlocksLocation() {
		return this.blockslocation;
	}
	
}
