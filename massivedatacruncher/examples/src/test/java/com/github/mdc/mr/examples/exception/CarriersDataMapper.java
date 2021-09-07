package com.github.mdc.mr.examples.exception;

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
		String[] contents = line.split(",");
		if (contents[0] != null && !contents[0].equals("Code")) {
			if (contents != null && contents.length > 1) {
				Map<String, String> map = new HashMap<>();
				map.put("CARRIERS", contents[1].substring(1, contents[1].length() - 1));
				ctx.put(contents[0].substring(1, contents[0].length() - 1), map);
			}
		}
	}

	@Override
	public void reduce(String key, List<Map> values, Context<String, String> context) {
		long arrdelaysum = 0, depdelaysum = 0;
		String carrierName = "";
		for (Map map : values) {
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
		Map map1 = new HashMap<>();
		context.put(key, carrierName + "," + (arrdelaysum == 0 ? "" : arrdelaysum) + ","
				+ (depdelaysum == 0 ? "" : depdelaysum));

	}

	@Override
	public void combine(String key, List<Map> values, Context<String, Map> context) {
		long arrdelaysum = 0, depdelaysum = 0;
		String carrierName = null;
		Map mapdelaywithcarrier = new HashMap<>();
		for (Map map : values) {
			if (map.get("CARRIERS") != null) {
				carrierName = (String) map.get("CARRIERS");
				mapdelaywithcarrier.put("CARRIERS", carrierName);
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
