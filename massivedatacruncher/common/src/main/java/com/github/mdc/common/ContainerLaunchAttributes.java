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
import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author arun
 *
 * The class with fields which contains various information for launching containers.
 */
public class ContainerLaunchAttributes implements Serializable {

	private static final long serialVersionUID = -949533421576520656L;
	private int numberofcontainers;
	List<ContainerResources> cr = new ArrayList<>();

	public int getNumberofcontainers() {
		return numberofcontainers;
	}

	public void setNumberofcontainers(int numberofcontainers) {
		this.numberofcontainers = numberofcontainers;
	}

	public List<ContainerResources> getCr() {
		return cr;
	}

	public void setCr(List<ContainerResources> cr) {
		this.cr = cr;
	}


}
