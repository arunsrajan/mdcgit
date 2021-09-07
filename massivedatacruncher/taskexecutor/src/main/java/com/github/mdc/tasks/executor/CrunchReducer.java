package com.github.mdc.tasks.executor;

import java.util.List;

public interface CrunchReducer<Ik,Iv,Context> {
	public void reduce(Ik ik,List<Iv> iv,Context context);
}
