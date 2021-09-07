package com.github.mdc.common;

import java.util.List;

public class TssHAHostPorts {
	private TssHAHostPorts() {};
	static List<String> hp;
	public static void set(List<String> hp) {
		TssHAHostPorts.hp = hp;
	}
	public static List<String> get() {
		return TssHAHostPorts.hp;
	}
}
