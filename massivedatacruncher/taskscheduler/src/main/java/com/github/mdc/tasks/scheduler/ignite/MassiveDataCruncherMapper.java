package com.github.mdc.tasks.scheduler.ignite;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.log4j.Logger;
import org.xerial.snappy.SnappyInputStream;

import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.Context;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.tasks.executor.CrunchMapper;

@SuppressWarnings("rawtypes")
public class MassiveDataCruncherMapper{
	@IgniteInstanceResource
	Ignite ignite;
	
	
	
	static Logger log = Logger.getLogger(MassiveDataCruncherMapper.class);
	BlocksLocation blockslocation;
	List<CrunchMapper> crunchmappers;
	public MassiveDataCruncherMapper(BlocksLocation blockslocation,List<CrunchMapper> crunchmappers) {
		this.blockslocation = blockslocation;
		this.crunchmappers = crunchmappers;
	}
	
	public Context execute() throws Exception {
		try(IgniteCache<Object,byte[]> cache = ignite.getOrCreateCache(MDCConstants.MDCCACHE);
				var compstream = new SnappyInputStream(new ByteArrayInputStream(cache.get(blockslocation)));
				var br = 
						new BufferedReader(new InputStreamReader(compstream));) {
			var ctx = new DataCruncherContext();
			br.lines().parallel().forEach(line->{
				for (CrunchMapper crunchmapper : crunchmappers) {
					crunchmapper.map(0l, line, ctx);
				}
			});
			return ctx;
		}
		catch(Exception ex) {
			log.info(MDCConstants.EMPTY,ex);
			throw ex;
		}
		
	}

	
	
}
