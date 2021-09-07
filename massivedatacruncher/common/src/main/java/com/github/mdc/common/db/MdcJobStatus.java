package com.github.mdc.common.db;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "mdc_job_status")
public class MdcJobStatus {
	
	@Id
	private Long id;
	
	@Column(name= "jobstatus")
	private Long status;
}
