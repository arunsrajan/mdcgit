package com.github.mdc.tasks.executor;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.github.mdc.common.Context;
import com.github.mdc.common.DataCruncherContext;

@SuppressWarnings("rawtypes")
public class ReducerExecutor implements Callable<Context> {

	static Logger log = Logger.getLogger(ReducerExecutor.class);
	DataCruncherContext dcc;
	Reducer cr;
	Object key;

	public ReducerExecutor(DataCruncherContext dcc, Reducer cr, Object key) {
		this.dcc = dcc;
		this.cr = cr;
		this.key = key;
	}

	@SuppressWarnings({"unchecked" })
	@Override
	public Context call() throws Exception {
		var ctx = new DataCruncherContext();
		dcc.keys().parallelStream().forEachOrdered(key->{
			cr.reduce(key, (List)dcc.get(key), ctx);
		});
		return ctx;
	}

}
