package com.github.mdc.mr.examples.join;

import java.util.HashMap;
import java.util.Map;

import com.github.mdc.common.Context;
import com.github.mdc.tasks.executor.Mapper;

public class AirlineDepDelayDataMapper implements Mapper<Long, String, Context<String, Map>> {

	@Override
	public void map(Long chunkid, String line, Context<String, Map> ctx) {
		try {
			if (line != null) {
				var contents = line.split(",");
				var map = new HashMap<String, Long>();
				if (contents != null && contents.length > 15 && contents[15] != null && !"NA".equals(contents[15]) && !"DepDelay".equals(contents[15])) {
					map.put("AIRLINEDEPDELAY", Long.parseLong(contents[15]));
				} else {
					map.put("AIRLINEDEPDELAY", null);
				}
				ctx.put(contents[8], map);
			}
		}
		catch (Exception ex) {

		}
	}

}
