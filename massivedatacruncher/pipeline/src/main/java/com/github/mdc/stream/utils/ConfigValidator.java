package com.github.mdc.stream.utils;

import java.util.List;

import com.github.mdc.common.PipelineConfig;

public interface ConfigValidator {
	public List<String> validate(PipelineConfig pc);
}
