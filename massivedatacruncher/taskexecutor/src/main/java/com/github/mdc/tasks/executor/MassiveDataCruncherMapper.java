package com.github.mdc.tasks.executor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.xerial.snappy.SnappyInputStream;

import com.github.mdc.common.BlocksLocation;
import com.github.mdc.common.Context;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.common.MDCConstants;

@SuppressWarnings("rawtypes")
public class MassiveDataCruncherMapper implements Callable<Context> {
	static Logger log = Logger.getLogger(MassiveDataCruncherMapper.class);
	BlocksLocation blockslocation;
	List<CrunchMapper> crunchmappers;
	SnappyInputStream datastream;
	public MassiveDataCruncherMapper(BlocksLocation blockslocation,SnappyInputStream datastream,List<CrunchMapper> crunchmappers) {
		this.blockslocation = blockslocation;
		this.datastream = datastream;
		this.crunchmappers = crunchmappers;
	}
	
	@Override
	public Context call() throws Exception {
		try(var compstream = datastream;
				var br = 
						new BufferedReader(new InputStreamReader(compstream));) {
			var ctx = new DataCruncherContext();
			br.lines().parallel().forEach(line->{
				for (var crunchmapper : crunchmappers) {
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
