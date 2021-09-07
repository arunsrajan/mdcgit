package com.github.mdc.common;

/**
 * 
 * @author Arun
 * Heartbeat life cycle methods.
 */
public sealed interface HeartBeatServerMBean permits HeartBeatServer,HeartBeatServerStream {
	public abstract void init(Object... config) throws Exception;
	public abstract void start() throws Exception;
	public abstract void send(byte[] data) throws Exception;
	public abstract void stop() throws Exception;
	public abstract void ping() throws Exception;
	public abstract void destroy() throws Exception;
	
	
}
