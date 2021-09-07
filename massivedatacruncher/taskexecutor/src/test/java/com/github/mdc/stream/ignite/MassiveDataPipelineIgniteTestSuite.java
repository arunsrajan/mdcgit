package com.github.mdc.stream.ignite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ MassiveDataPipelineIgniteDepth2Test.class, MassiveDataPipelineIgniteDepth31Test.class,
		MassiveDataPipelineIgniteDepth32ContinuedTest.class, MassiveDataPipelineIgniteDepth32Test.class,
		MassiveDataPipelineIgniteDepth33Test.class, MassiveDataPipelineIgniteDepth34Test.class })
public class MassiveDataPipelineIgniteTestSuite {

}
