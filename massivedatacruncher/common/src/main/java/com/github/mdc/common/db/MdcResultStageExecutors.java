package com.github.mdc.common.db;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Table(name = "stage_executors")
public class MdcResultStageExecutors {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long id;
	
	@Column(insertable = false,updatable = false,name="mdcresultid")
	private Long mdcResultId;
	
	@Column(name="stage_id")
	private String stageId;
	
	@Column(name="executor_info")
	private String executorInfo;
	
	@Column(name="stage_execution_submitted_time")
	private Timestamp stageExecutionSubmittedTime;
	
	@Column(name="stage_execution_completion_time")
	private Timestamp stageExecutionCompletionTime;
	
	
	@ManyToOne
    @JoinColumn(name="mdcresultid", nullable=false)
    private MdcPipelineJobResult result;


	public Long getId() {
		return id;
	}


	public void setId(Long id) {
		this.id = id;
	}


	public Long getMdcResultId() {
		return mdcResultId;
	}


	public void setMdcResultId(Long mdcResultId) {
		this.mdcResultId = mdcResultId;
	}


	public String getStageId() {
		return stageId;
	}


	public void setStageId(String stageId) {
		this.stageId = stageId;
	}


	public String getExecutorInfo() {
		return executorInfo;
	}


	public void setExecutorInfo(String executorInfo) {
		this.executorInfo = executorInfo;
	}


	public Timestamp getStageExecutionSubmittedTime() {
		return stageExecutionSubmittedTime;
	}


	public void setStageExecutionSubmittedTime(Timestamp stageExecutionSubmittedTime) {
		this.stageExecutionSubmittedTime = stageExecutionSubmittedTime;
	}


	public Timestamp getStageExecutionCompletionTime() {
		return stageExecutionCompletionTime;
	}


	public void setStageExecutionCompletionTime(Timestamp stageExecutionCompletionTime) {
		this.stageExecutionCompletionTime = stageExecutionCompletionTime;
	}


	public MdcPipelineJobResult getResult() {
		return result;
	}


	public void setResult(MdcPipelineJobResult result) {
		this.result = result;
	}
	
	
	
	
}
