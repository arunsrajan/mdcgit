package com.github.mdc.stream.utils;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import com.github.mdc.common.PipelineConfig;
import com.github.mdc.common.Utils;
import com.github.mdc.stream.utils.PipelineConfigValidator;

public class PipelineConfigValidatorTest {
	
	@Test
	public void testBlockSizeNumberError() throws Exception {
			PipelineConfigValidator configvalidator = new PipelineConfigValidator();
			PipelineConfig config = getPipelineConfig();
			config.setBlocksize("BlockSize");
			List<String> errormsgs = configvalidator.validate(config);
			assertEquals(PipelineConfigValidator.BLOCKSIZENUMBER,errormsgs.get(0));
	}
	@Test
	public void testBlockSizeRangeErrorPositiveGreaterThan256() throws Exception {
			PipelineConfigValidator configvalidator = new PipelineConfigValidator();
			PipelineConfig config = getPipelineConfig();
			config.setBlocksize("512");
			List<String> errormsgs = configvalidator.validate(config);
			assertEquals(PipelineConfigValidator.BLOCKSIZERANGE,errormsgs.get(0));
	}
	@Test
	public void testBatchSizeNumberError() throws Exception {
			PipelineConfigValidator configvalidator = new PipelineConfigValidator();
			PipelineConfig config = getPipelineConfig();
			config.setBatchsize("BlockSize");
			List<String> errormsgs = configvalidator.validate(config);
			assertEquals(PipelineConfigValidator.BATCHSIZENUMBER,errormsgs.get(0));
	}
	@Test
	public void testMesosTrueFalseError() throws Exception {
			PipelineConfigValidator configvalidator = new PipelineConfigValidator();
			PipelineConfig config = getPipelineConfig();
			config.setMesos("OtherThanTrueFalse");
			List<String> errormsgs = configvalidator.validate(config);
			assertEquals(PipelineConfigValidator.MESOSTRUEFALSE,errormsgs.get(0));
	}
	@Test
	public void testYARNTrueFalseError() throws Exception {
			PipelineConfigValidator configvalidator = new PipelineConfigValidator();
			PipelineConfig config = getPipelineConfig();
			config.setYarn("OtherThanTrueFalse");
			List<String> errormsgs = configvalidator.validate(config);
			assertEquals(PipelineConfigValidator.YARNTRUEFALSE,errormsgs.get(0));
	}
	@Test
	public void testLocalTrueFalseError() throws Exception {
			PipelineConfigValidator configvalidator = new PipelineConfigValidator();
			PipelineConfig config = getPipelineConfig();
			config.setLocal("OtherThanTrueFalse");
			List<String> errormsgs = configvalidator.validate(config);
			assertEquals(PipelineConfigValidator.LOCALTRUEFALSE,errormsgs.get(0));
	}
	@Test
	public void testJGroupsTrueFalseError() throws Exception {
			PipelineConfigValidator configvalidator = new PipelineConfigValidator();
			PipelineConfig config = getPipelineConfig();
			config.setJgroups("OtherThanTrueFalse");
			List<String> errormsgs = configvalidator.validate(config);
			assertEquals(PipelineConfigValidator.JGROUPSTRUEFALSE,errormsgs.get(0));
	}
	@Test
	public void testOneModeSetError() throws Exception {
			PipelineConfigValidator configvalidator = new PipelineConfigValidator();
			PipelineConfig config = getPipelineConfig();
			config.setJgroups("true");
			config.setYarn("true");
			List<String> errormsgs = configvalidator.validate(config);
			assertEquals(PipelineConfigValidator.ERRORJGROUPSYARNLOCALMESOSSET,errormsgs.get(0));
	}
	@Test
	public void testMinMemValueNotNumberError() throws Exception {
			PipelineConfigValidator configvalidator = new PipelineConfigValidator();
			PipelineConfig config = getPipelineConfig();
			config.setMinmem("NotNumeric");
			List<String> errormsgs = configvalidator.validate(config);
			assertEquals(PipelineConfigValidator.MINMEMNUMBER,errormsgs.get(0));
	}
	@Test
	public void testMaxMemValueNotNumberError() throws Exception {
			PipelineConfigValidator configvalidator = new PipelineConfigValidator();
			PipelineConfig config = getPipelineConfig();
			config.setMaxmem("NotNumeric");
			List<String> errormsgs = configvalidator.validate(config);
			assertEquals(PipelineConfigValidator.MAXMEMNUMBER,errormsgs.get(0));
	}
	@Test
	public void testGCError() throws Exception {
			PipelineConfigValidator configvalidator = new PipelineConfigValidator();
			PipelineConfig config = getPipelineConfig();
			config.setGctype("NotAValidGcType");
			List<String> errormsgs = configvalidator.validate(config);
			assertEquals(PipelineConfigValidator.GCTYPEERROR,errormsgs.get(0));
	}
	@Test
	public void testNumContError() throws Exception {
			PipelineConfigValidator configvalidator = new PipelineConfigValidator();
			PipelineConfig config = getPipelineConfig();
			config.setNumberofcontainers("NoANumber");
			List<String> errormsgs = configvalidator.validate(config);
			assertEquals(PipelineConfigValidator.NUMCONTERROR,errormsgs.get(0));
	}
	@Test
	public void testMesosUrlNotSetError() throws Exception {
			PipelineConfigValidator configvalidator = new PipelineConfigValidator();
			PipelineConfig config = getPipelineConfig();
			config.setLocal("false");
			config.setMesos("true");
			config.setMesosmaster("");
			List<String> errormsgs = configvalidator.validate(config);
			assertEquals(PipelineConfigValidator.MESOSMASTERNOTSETERROR,errormsgs.get(0));
	}
	public PipelineConfig getPipelineConfig() throws Exception {
		Utils.loadLog4JSystemPropertiesClassPath("mdctest.properties");
		PipelineConfig config = new PipelineConfig();
		return config;
	}
	
}
