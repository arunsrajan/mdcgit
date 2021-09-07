package com.github.mdc.stream;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.github.mdc.common.Job;
import com.github.mdc.common.JobMetrics;
import com.github.mdc.common.MDCConstants;
import com.github.mdc.common.PipelineConfig;
import com.github.mdc.stream.MassiveDataPipeline;

public class MassiveDataPipelineTest extends MDCPipelineTestsCommon{
	
	@Test
	public void testGetDAG() throws Exception {
		PipelineConfig pc = new PipelineConfig();
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsurl,hdfsdirpaths1[0], pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val)->val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);
		Job job = new Job();
		job.jm = new JobMetrics();
		mdparr.getDAG(job);
		assertEquals(1, job.stageoutputmap.size());
		assertEquals(1, job.topostages.size());
	}

	@Test
	public void testFormDAGAbstractFunction() throws Exception {
		PipelineConfig pc = new PipelineConfig();
		MassiveDataPipeline<String> mdp = MassiveDataPipeline.newStreamHDFS(hdfsurl,hdfsdirpaths1[0], pc);
		MassiveDataPipeline<String[]> mdparr = mdp.map((val)->val.split(MDCConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);
		mdparr.formDAGAbstractFunction(null, mdparr.mdsroots);
		assertEquals(2, mdparr.graph.vertexSet().size());
	}
}
