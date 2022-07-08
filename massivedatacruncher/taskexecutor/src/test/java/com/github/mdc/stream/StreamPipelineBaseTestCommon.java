package com.github.mdc.stream;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class StreamPipelineBaseTestCommon extends StreamPipelineBase {
	@BeforeClass
	public static void setServerUp() throws Exception {
		if (!setupdone) {
			StreamPipelineTestSuite.setServerUp();
			setupdone = true;
			toteardownclass = true;
		}
	}
	@AfterClass
	public static void tearDown() throws Exception {
		if (toteardownclass) {
			StreamPipelineTestSuite.closeResources();
		}
	}
}
