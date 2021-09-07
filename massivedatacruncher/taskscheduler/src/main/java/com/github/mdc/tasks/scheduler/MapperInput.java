package com.github.mdc.tasks.scheduler;

public class MapperInput {
	@SuppressWarnings("rawtypes")
	public MapperInput(Class mapper,String inputfolderpath) {
		this.crunchmapper = mapper;
		this.inputfolderpath = inputfolderpath;
	}
	@SuppressWarnings("rawtypes")
	Class crunchmapper;
	String inputfolderpath;
}
