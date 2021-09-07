package com.github.mdc.tasks.scheduler.ignite;

import java.util.List;
import java.util.Set;

import org.apache.ignite.lang.IgniteCallable;
import org.apache.log4j.Logger;

import com.github.mdc.common.Context;
import com.github.mdc.common.DataCruncherContext;
import com.github.mdc.tasks.executor.CrunchCombiner;

@SuppressWarnings("rawtypes")
public class MassiveDataCruncherCombiner implements IgniteCallable<Context> {
	private static final long serialVersionUID = -2886619127389224706L;
	
	static Logger log = Logger.getLogger(MassiveDataCruncherCombiner.class);
	Context dcc;
	CrunchCombiner cc;

	public MassiveDataCruncherCombiner(Context dcc, CrunchCombiner cc) {
		this.dcc = dcc;
		this.cc = cc;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Context call() throws Exception {
		Set<Object> keys = dcc.keys();
		var ctx = new DataCruncherContext();
		keys.stream().parallel().forEach(key->{
			cc.combine(key, (List)dcc.get(key), ctx);
			
		});
		return ctx;
	}

}
