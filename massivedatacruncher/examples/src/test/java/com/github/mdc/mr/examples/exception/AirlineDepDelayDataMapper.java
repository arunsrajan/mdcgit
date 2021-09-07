package com.github.mdc.mr.examples.exception;

import java.util.HashMap;
import java.util.Map;

import com.github.mdc.common.Context;
import com.github.mdc.tasks.executor.CrunchMapper;

public class AirlineDepDelayDataMapper implements CrunchMapper<Long, String, Context<String, Map>> {

	@Override
	public void map(Long chunkid, String line, Context<String, Map> ctx) {
		try {
			String[] contents = line.split(",");
			Map<String, Long> map = new HashMap<>();
			if (contents!=null && contents.length>15 && contents[15]!=null && !contents[15].equals("NA") && !contents[15].equals("DepDelay")) {
				map.put("AIRLINEDEPDELAY", Long.parseLong(contents[15]));
			}else {
				map.put("AIRLINEDEPDELAY", null);
			}
			ctx.put(contents[8], map);
		}
		catch(Exception ex) {
			
		}
	}

}
