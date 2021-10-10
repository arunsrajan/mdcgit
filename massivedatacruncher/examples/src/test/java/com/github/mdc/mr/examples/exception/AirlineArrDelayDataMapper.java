package com.github.mdc.mr.examples.exception;

import java.util.HashMap;
import java.util.Map;

import com.github.mdc.common.Context;
import com.github.mdc.tasks.executor.Mapper;

public class AirlineArrDelayDataMapper implements Mapper<Long, String, Context<String, Map>> {

	@Override
	public void map(Long chunkid, String line, Context<String, Map> ctx) {
		String[] contents = line.split(",");
		Map<String, Long> map = new HashMap<>();
		
		if (contents != null && contents.length > 14 && contents[14] != null && !contents[14].equals("NA") && !contents[14].equals("ArrDelay")) {
			map.put("AIRLINEARRDELAY", Long.parseLong(contents[14]));
			ctx.put(contents[8], map);
		}

	}

}
