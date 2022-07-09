package com.github.mdc.tasks.executor;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.github.mdc.common.Context;
import com.github.mdc.common.DataCruncherContext;

@SuppressWarnings("rawtypes")
public class CombinerExecutor implements Callable<Context> {
	static Logger log = Logger.getLogger(CombinerExecutor.class);
	Context dcc;
	Combiner cc;

	public CombinerExecutor(Context dcc, Combiner cc) {
		this.dcc = dcc;
		this.cc = cc;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Context call() throws Exception {
		Set<Object> keys = dcc.keys();
		var ctx = new DataCruncherContext();
		keys.stream().parallel().forEach(key -> {
			cc.combine(key, (List) dcc.get(key), ctx);

		});
		return ctx;
	}

}
