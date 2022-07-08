package com.github.mdc.stream.ignite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({IgnitePipelineDepth2Test.class, IgnitePipelineDepth31Test.class,
		IgnitePipelineDepth32ContinuedTest.class, IgnitePipelineDepth32Test.class,
		IgnitePipelineDepth33Test.class, IgnitePipelineDepth34Test.class})
public class IgnitePipelineTestSuite {

}
