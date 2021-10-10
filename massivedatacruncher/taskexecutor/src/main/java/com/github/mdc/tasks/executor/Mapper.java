package com.github.mdc.tasks.executor;

public interface Mapper<Ik,Iv,C> {

	public void map(Ik ik,Iv iv,C ctx);
	
}
