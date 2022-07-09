package com.github.mdc.common;

import java.io.Serializable;

public class RemoteDataWriterTask implements Serializable {
	public static enum ResultWriteStatus {
		SUCCESS,FAILURE
	}
	private static final long serialVersionUID = 5797571384236793506L;
	public String hdfsurl;
	public String filepath;
	public RemoteDataFetch rdf;
	public ResultWriteStatus status;
}
