package com.github.mdc.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class UtilsTest {

	@Test
	public void testaddShutDownHook() {
		Utils.addShutdownHook(() -> {
		});
	}

	@Test
	public void testloadLog4JSystemPropertiesPropertyFileNull() {
		try {
			Utils.loadLog4JSystemProperties("", null);
		}
		catch (Exception ex) {
			assertEquals("Property File Name cannot be null", ex.getMessage());
		}
	}

	@Test
	public void testloadLog4JSystemPropertiesFilePath() {
		try {
			Utils.loadLog4JSystemProperties(null, "");
		}
		catch (Exception ex) {
			assertEquals("Properties File Path cannot be null", ex.getMessage());
		}
	}

	@Test
	public void testloadLog4JSystemPropertiesProperInput() throws Exception {
		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.BACKWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.BACKWARD_SLASH, MDCConstants.MDC_PROPERTIES);
	}

	@Test
	public void testloadLog4JSystemPropertiesEmptyFileName() {
		try {
			Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.BACKWARD_SLASH
					+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.BACKWARD_SLASH, "");
		}
		catch (Exception ex) {
			assertEquals("Unable To Load Properties", ex.getMessage());
			assertTrue(ex.getCause().getMessage().contains("Access is denied"));
		}
	}

	@Test
	public void testloadLog4JSystemPropertiesClassPathImproperFileName() {
		try {
			Utils.loadLog4JSystemPropertiesClassPath(null);
		}
		catch (Exception ex) {
			assertEquals("Property File Name cannot be null", ex.getMessage());
		}
	}

	@Test
	public void testloadLog4JSystemPropertiesClassPathEmptyFileName() {
		try {
			Utils.loadLog4JSystemPropertiesClassPath("");
		}
		catch (Exception ex) {
			assertEquals("Unable To Load Properties", ex.getMessage());
			assertTrue(ex.getCause().getMessage().contains("Access is denied"));
		}
	}

	@Test
	public void testloadLog4JSystemPropertiesClassPathProperInput() throws Exception {
		Utils.loadLog4JSystemPropertiesClassPath(MDCConstants.MDC_TEST_PROPERTIES);
	}
}
