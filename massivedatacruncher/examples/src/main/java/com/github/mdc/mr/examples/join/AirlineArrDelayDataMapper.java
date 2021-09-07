package com.github.mdc.mr.examples.join;

import java.util.HashMap;
import java.util.Map;

import com.github.mdc.common.Context;
import com.github.mdc.tasks.executor.CrunchMapper;

public class AirlineArrDelayDataMapper implements CrunchMapper<Long, String, Context<String, Map>> {

	@Override
	public void map(Long chunkid, String line, Context<String, Map> ctx) {
		var contents = line.split(",");
		var map = new HashMap<>();
		
		if (contents != null && contents.length > 14 && contents[14] != null && !contents[14].equals("NA") && !contents[14].equals("ArrDelay")) {
			map.put("AIRLINEARRDELAY", Long.parseLong(contents[14]));
			ctx.put(contents[8], map);
		}

	}

}
