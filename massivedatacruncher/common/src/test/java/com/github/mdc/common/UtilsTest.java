/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
		Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
				+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, MDCConstants.MDC_PROPERTIES);
	}

	@Test
	public void testloadLog4JSystemPropertiesEmptyFileName() {
		try {
			Utils.loadLog4JSystemProperties(MDCConstants.PREV_FOLDER + MDCConstants.FORWARD_SLASH
					+ MDCConstants.DIST_CONFIG_FOLDER + MDCConstants.FORWARD_SLASH, "");
		}
		catch (Exception ex) {
			assertEquals("Unable To Load Properties", ex.getMessage());
			assertTrue(ex.getCause().getMessage().contains("Access is denied"));
		}
	}

}
