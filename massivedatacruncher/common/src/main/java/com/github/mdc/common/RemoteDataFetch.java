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

import java.io.Serializable;
import java.util.Arrays;

/**
 * 
 * @author Arun
 * The Holder of job and stage information to receive the final stage output.
 */
public class RemoteDataFetch implements Serializable {
	private static final long serialVersionUID = 2952764365767007054L;
	public String jobid;
	public String stageid;
	public String taskid;
	public String hp;
	public byte[] data;
	public String mode;

	@Override
	public String toString() {
		return "RemoteDataFetch [jobid=" + jobid + ", stageid=" + stageid + ", taskid=" + taskid + ", hp=" + hp
				+ ", data=" + Arrays.toString(data) + ", mode=" + mode + "]";
	}
}
