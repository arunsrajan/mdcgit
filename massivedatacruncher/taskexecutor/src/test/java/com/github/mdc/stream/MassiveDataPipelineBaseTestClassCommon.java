package com.github.mdc.stream;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public class MassiveDataPipelineBaseTestClassCommon extends MassiveDataPipelineBase {
	@BeforeClass
	public static void setServerUp() throws Exception {
		if(!setupdone) {
			MassiveDataCruncherTestSuite.setServerUp();
			setupdone = true;
			toteardownclass = true;
		}
	}
	@AfterClass
	public static void tearDown() throws Exception {
		if(toteardownclass) {
			MassiveDataCruncherTestSuite.closeResources();
		}
	}
}
