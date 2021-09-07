package com.github.mdc.tasks.executor;

import java.util.List;

public interface CrunchCombiner <Ik,Iv,Context> {
	public void combine(Ik ik,List<Iv> iv,Context context);

}
