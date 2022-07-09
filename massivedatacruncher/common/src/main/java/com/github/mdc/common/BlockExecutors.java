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
