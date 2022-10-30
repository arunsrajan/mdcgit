package com.github.mdc.common;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Response received from task executors heartbeat
 * @author arun
 *
 */
@Getter
@Setter
@EqualsAndHashCode
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ResponseReceived implements Serializable {

	private static final long serialVersionUID = 1L;
	private String hp;
	
}
