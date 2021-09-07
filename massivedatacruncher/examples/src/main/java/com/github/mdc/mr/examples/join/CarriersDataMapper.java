package com.github.mdc.mr.examples.join;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.mdc.common.Context;
import com.github.mdc.tasks.executor.CrunchCombiner;
import com.github.mdc.tasks.executor.CrunchMapper;
import com.github.mdc.tasks.executor.CrunchReducer;

public class CarriersDataMapper implements CrunchMapper<Long, String, Context<String, Map>>,
		CrunchReducer<String, Map, Context<String, String>>, CrunchCombiner<String, Map, Context<String, Map>> {

	@Override
	public void map(Long chunkid, String line, Context<String, Map> ctx) {
		var contents = line.split(",");
		if (contents[0] != null && !contents[0].equals("Code")) {
			if (contents != null && contents.length > 1) {
				var map = new HashMap<String, String>();
				map.put("CARRIERS", contents[1].substring(1, contents[1].length() - 1));
				ctx.put(contents[0].substring(1, contents[0].length() - 1), map);
			}
		}
	}

	@Override
	public void reduce(String key, List<Map> values, Context<String, String> context) {
		var arrdelaysum = 0l;
		var depdelaysum = 0l;
		var carrierName = "";
		for (var map : values) {
			if (map.get("CARRIERS") != null) {
				carrierName = (String) map.get("CARRIERS");
			}
			if (map.get("AIRLINEARRDELAY") != null) {
				arrdelaysum += (long) map.get("AIRLINEARRDELAY");
			} 
			if (map.get("AIRLINEDEPDELAY") != null) {
				depdelaysum += (long) map.get("AIRLINEDEPDELAY");
			}
		}
		var map1 = new HashMap<>();
		if(!(arrdelaysum==0&&depdelaysum == 0)) {
			context.put(key, carrierName + "," + (arrdelaysum == 0 ? "" : arrdelaysum) + ","
				+ (depdelaysum == 0 ? "" : depdelaysum));
		}

	}

	@Override
	public void combine(String key, List<Map> values, Context<String, Map> context) {
		var arrdelaysum = 0l;
		var depdelaysum = 0l;
		var mapdelaywithcarrier = new HashMap<>();
		for (var map : values) {
			if (map.get("CARRIERS") != null) {
				mapdelaywithcarrier.put("CARRIERS", map.get("CARRIERS"));
			} 
			if (map.get("AIRLINEARRDELAY") != null) {
				arrdelaysum += (long) map.get("AIRLINEARRDELAY");
			} 
			if (map.get("AIRLINEDEPDELAY") != null) {
				depdelaysum += (long) map.get("AIRLINEDEPDELAY");
			}
		}
		
		mapdelaywithcarrier.put("AIRLINEARRDELAY", arrdelaysum);
		mapdelaywithcarrier.put("AIRLINEDEPDELAY", depdelaysum);
		context.put(key, mapdelaywithcarrier);
	}

}
