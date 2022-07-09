package com.github.mdc.common;

public interface ServerUtilsMBean {
	public abstract void init(Object... config) throws Exception;

	public abstract void start() throws Exception;

	public abstract void stop() throws Exception;

	public abstract void destroy() throws Exception;
}
