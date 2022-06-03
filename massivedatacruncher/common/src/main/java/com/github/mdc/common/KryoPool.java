package com.github.mdc.common;

import java.util.Objects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.util.Pool;

public class KryoPool {

	private KryoPool() {

	}

	private static Pool<Kryo> kryopool = null;

	public static Pool<Kryo> getKryoPool() {
		if (Objects.isNull(kryopool))
			kryopool = new Pool<Kryo>(true, false, 8) {
				protected Kryo create() {
					return Utils.getKryoNonDeflateSerializer();
				}
			};
		return kryopool;
	}

}
