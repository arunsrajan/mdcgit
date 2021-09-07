package com.github.mdc.tasks.scheduler.yarn;

import java.util.Set;

import com.github.mdc.common.ApplicationTask;
import com.github.mdc.common.BlocksLocation;

public class MapperCombiner {
	BlocksLocation blockslocation;
	Set<String> mapperclasses;
	ApplicationTask apptask;
	Set<String> combinerclasses;

	public MapperCombiner(BlocksLocation blockslocation, Set<String> mapperclasses, ApplicationTask apptask,
			Set<String> combinerclasses) {
		this.blockslocation = blockslocation;
		this.mapperclasses = mapperclasses;
		this.apptask = apptask;
		this.combinerclasses = combinerclasses;
	}

	public BlocksLocation getBlockslocation() {
		return blockslocation;
	}

	public void setBlockslocation(BlocksLocation blockslocation) {
		this.blockslocation = blockslocation;
	}

	public Set<String> getMapperclasses() {
		return mapperclasses;
	}

	public void setMapperclasses(Set<String> mapperclasses) {
		this.mapperclasses = mapperclasses;
	}

	public ApplicationTask getApptask() {
		return apptask;
	}

	public void setApptask(ApplicationTask apptask) {
		this.apptask = apptask;
	}

	public Set<String> getCombinerclasses() {
		return combinerclasses;
	}

	public void setCombinerclasses(Set<String> combinerclasses) {
		this.combinerclasses = combinerclasses;
	}

	@Override
	public String toString() {
		return "MapperCombiner [blockslocation=" + blockslocation + ", mapperclasses=" + mapperclasses + ", apptask="
				+ apptask + ", combinerclasses=" + combinerclasses + "]";
	}
	
	
	
	
}
