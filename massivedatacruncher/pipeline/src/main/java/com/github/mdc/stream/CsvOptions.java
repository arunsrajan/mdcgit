package com.github.mdc.stream;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 
 * @author arun
 * This class is options for CSV based pipeline.
 */
public class CsvOptions implements Serializable {
	private static final long serialVersionUID = -4005311056383759864L;

	public CsvOptions() {
		super();
	}
	private String[] header;
	private ConcurrentMap<String, Integer> headerindexmap = new ConcurrentHashMap<>();

	public CsvOptions(String[] header) {
		this.header = header;
		int count = 0;
		for (String head :header) {
			headerindexmap.put(head, count);
			count++;
		}
	}

	public String get(int headerindex) {
		return header[headerindex];
	}

	public int getIndexForHeader(String headerindex) {
		return headerindexmap.get(headerindex);
	}

	public String[] getHeader() {
		return header;
	}
}
