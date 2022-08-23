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

public class BlockExecutors {
	public BlockExecutors(String hp, int numberofblockstoread) {
		this.hp = hp;
		this.numberofblockstoread = numberofblockstoread;
	}
	String hp;
	int numberofblockstoread;

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}

		if (this.getClass() != obj.getClass()) {
			return false;
		}
		BlockExecutors be = (BlockExecutors) obj;
		return be.hp.equals(this.hp) && be.numberofblockstoread == this.numberofblockstoread;

	}

	@Override
	public int hashCode() {
		return this.hp.hashCode();
	}

	public String toString() {
		return hp + MDCConstants.HYPHEN + numberofblockstoread;
	}
}
