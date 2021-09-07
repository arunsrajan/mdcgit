package com.github.mdc.tasks.scheduler.ignite;

import java.util.List;

import org.apache.ignite.lang.IgniteCallable;
import org.apache.log4j.Logger;

import com.github.mdc.common.Context;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.tasks.executor.CrunchReducer;

@SuppressWarnings("rawtypes")
public class MassiveDataCruncherReducer implements IgniteCallable<Context> {

	private static final long serialVersionUID = -1246953663442464999L;
	static Logger log = Logger.getLogger(MassiveDataCruncherReducer.class);
	DataCruncherContext dcc;
	CrunchReducer cr;

	public MassiveDataCruncherReducer(DataCruncherContext dcc, CrunchReducer cr) {
		this.dcc = dcc;
		this.cr = cr;
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
