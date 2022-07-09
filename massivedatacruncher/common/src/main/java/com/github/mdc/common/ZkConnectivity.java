package com.github.mdc.common;

public interface ZkConnectivity<CF, CL> {
	public abstract void addConnectionStateListener(CF cf, CL cl);
}
