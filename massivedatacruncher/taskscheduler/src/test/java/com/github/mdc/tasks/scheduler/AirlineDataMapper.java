package com.github.mdc.tasks.scheduler;

import java.util.List;

import com.github.mdc.common.Context;
import com.github.mdc.tasks.executor.Combiner;
import com.github.mdc.tasks.executor.Mapper;
import com.github.mdc.tasks.executor.Reducer;

@SuppressWarnings("rawtypes")
public class AirlineDataMapper implements Mapper<Long, String, Context>, Combiner<String, Long, Context>,
Reducer<String, Long, Context>{

	@SuppressWarnings("unchecked")
	@Override
	public void combine(String key, List<Long> values, Context context) {
		long sum = 0;
		for (Long value :values) {
			sum += value;
		}
		context.put(key, sum);
	
	}

	@SuppressWarnings("unchecked")
	@Override
	public void map(Long index, String line, Context context) {

		String[] contents = line.split(",");
		if (contents[0] != null && !"Year".equals(contents[0])) {
			if (contents != null && contents.length > 14 && contents[14] != null && !"NA".equals(contents[14])) {
				context.put(contents[8], Long.parseLong(contents[14]));
			}
		}
	
		
	}

	@Override
	public void reduce(String key, List<Long> values, Context context) {
		long sum = 0;
		for (Long value :values) {
			sum += value;
		}
		context.put(key, sum);
		
	}

}
