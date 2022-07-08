package com.github.mdc.common;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * 
 * @author Arun
 * The tokenization of bytes to string
 */
public class LineSplitUtil {

	/**
	 * The calculation of line count by passing bytes.
	 * @param byt
	 * @return
	 * @throws Exception
	 */
	public static Long splitcount(byte[] byt) throws Exception {
		var count = 0l;
		for (var bt :byt) {
			if (bt == '\n') {
				count++;
			}
		}
		if (byt[byt.length - 1] != '\n') {
			count++;
		}
		return count;
	}
	
	/**
	 * Splitting of lines from bytes.
	 * @param byt
	 * @return
	 * @throws Exception
	 */
	public static String[] split(byte[] byt) throws Exception {
		var reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(byt)));
		String line;
		var strings = new ArrayList<>();
		while ((line = getNextLine(reader)) != null) {
			strings.add(line);
		}
		reader.close();
		return strings.toArray(new String[strings.size()]);
	}
	public static String getNextLine(BufferedReader reader) throws Exception {
		return reader.readLine();
	}

	private LineSplitUtil() {
	}
}
