package com.github.mdc.common;

import java.io.Serializable;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Task Executor Startup port and Shutdown port
 * @author arun
 *
 */
@EqualsAndHashCode
@Getter
@Setter
@ToString
public class TaskExecutorShutdown implements Serializable{

	private static final long serialVersionUID = 8673890517000699908L;
	
}
