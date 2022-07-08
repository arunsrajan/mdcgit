package com.github.mdc.mr.examples.join;

import java.util.HashMap;
import java.util.Map;

import com.github.mdc.common.Context;
import com.github.mdc.tasks.executor.Mapper;

public class AirlineArrDelayDataMapper implements Mapper<Long, String, Context<String, Map>> {

	@Override
	public void map(Long chunkid, String line, Context<String, Map> ctx) {
		var contents = line.split(",");
		var map = new HashMap<>();
		
		if (contents != null && contents.length > 14 && contents[14] != null && !"NA".equals(contents[14]) && !"ArrDelay".equals(contents[14])) {
			map.put("AIRLINEARRDELAY", Long.parseLong(contents[14]));
			ctx.put(contents[8], map);
		}

	}

}
