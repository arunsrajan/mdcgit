package com.github.mdc.common.db;

import java.sql.Timestamp;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

@Table(name = "mdc_pipeline_job_result")
@Entity
public class MdcPipelineJobResult {

	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	@Column(name="mdc_pipeline_result_id")
	private Long id;
	
	@Column(name="jobid")
	private String jobid;
	
	@Column(name = "created_date")
	private Timestamp createdDate;
	
	@Column(name = "modified_date")
	private Timestamp modifiedDate;
	
	@Column(name = "pipeline_start_date")
	private Timestamp pipelineExecutionStartDate;
	
	@Column(name = "pipeline_end_date")
	private Timestamp pipelineExecutionEndDate;
	
	@Column(name = "pipeline_result_url")
	private Timestamp pipelineResultUrl;
	
	@Column( length = 65535,name = "pipeline_intermediate_task_graph" )
	private String intermediateTaskGraphs;
	
	@Column( length = 65535,name = "pipeline_intermediate_stage_graph")
	private String intermediateStageTaskGraphs;
	
	@Column(name = "pipeline_initial_stages")
	private String intermediateInitialStages;
	
	@Column(name = "pipeline_final_stages")
	private String intermediateFinalStages;
	
	@Column(name = "no_of_partitions")
	private Long noOfPartitions;
	
	@Column(name = "no_of_task_executors")
	private Long noOfTaskExecutors;
	
	@Column(name = "task_executors_details")
	private String taskExcutorsDetail;
	
	@Column(name = "task_scheduler_details")
	private String taskSchedulerDetails;
	
	@Column(name = "task_jar_submitted")
	private String taskJarSubmitted;
	
	@Column(name = "job_status")
	private String jobStatus;
	
	@OneToMany(mappedBy="mdcResultId")
    private Set<MdcResultStageExecutors> stageExecutors;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Timestamp getCreatedDate() {
		return createdDate;
	}

	public void setCreatedDate(Timestamp createdDate) {
		this.createdDate = createdDate;
	}

	public Timestamp getModifiedDate() {
		return modifiedDate;
	}

	public void setModifiedDate(Timestamp modifiedDate) {
		this.modifiedDate = modifiedDate;
	}

	public Timestamp getPipelineExecutionStartDate() {
		return pipelineExecutionStartDate;
	}

	public void setPipelineExecutionStartDate(Timestamp pipelineExecutionStartDate) {
		this.pipelineExecutionStartDate = pipelineExecutionStartDate;
	}

	public Timestamp getPipelineExecutionEndDate() {
		return pipelineExecutionEndDate;
	}

	public void setPipelineExecutionEndDate(Timestamp pipelineExecutionEndDate) {
		this.pipelineExecutionEndDate = pipelineExecutionEndDate;
	}

	public Timestamp getPipelineResultUrl() {
		return pipelineResultUrl;
	}

	public void setPipelineResultUrl(Timestamp pipelineResultUrl) {
		this.pipelineResultUrl = pipelineResultUrl;
	}

	public String getIntermediateTaskGraphs() {
		return intermediateTaskGraphs;
	}

	public void setIntermediateTaskGraphs(String intermediateTaskGraphs) {
		this.intermediateTaskGraphs = intermediateTaskGraphs;
	}

	public String getIntermediateStageTaskGraphs() {
		return intermediateStageTaskGraphs;
	}

	public void setIntermediateStageTaskGraphs(String intermediateStageTaskGraphs) {
		this.intermediateStageTaskGraphs = intermediateStageTaskGraphs;
	}

	public String getIntermediateInitialStages() {
		return intermediateInitialStages;
	}

	public void setIntermediateInitialStages(String intermediateInitialStages) {
		this.intermediateInitialStages = intermediateInitialStages;
	}

	public String getIntermediateFinalStages() {
		return intermediateFinalStages;
	}

	public void setIntermediateFinalStages(String intermediateFinalStages) {
		this.intermediateFinalStages = intermediateFinalStages;
	}

	public Long getNoOfPartitions() {
		return noOfPartitions;
	}

	public void setNoOfPartitions(Long noOfPartitions) {
		this.noOfPartitions = noOfPartitions;
	}

	public Long getNoOfTaskExecutors() {
		return noOfTaskExecutors;
	}

	public void setNoOfTaskExecutors(Long noOfTaskExecutors) {
		this.noOfTaskExecutors = noOfTaskExecutors;
	}

	public String getTaskExcutorsDetail() {
		return taskExcutorsDetail;
	}

	public void setTaskExcutorsDetail(String taskExcutorsDetail) {
		this.taskExcutorsDetail = taskExcutorsDetail;
	}

	public String getTaskSchedulerDetails() {
		return taskSchedulerDetails;
	}

	public void setTaskSchedulerDetails(String taskSchedulerDetails) {
		this.taskSchedulerDetails = taskSchedulerDetails;
	}

	public String getTaskJarSubmitted() {
		return taskJarSubmitted;
	}

	public void setTaskJarSubmitted(String taskJarSubmitted) {
		this.taskJarSubmitted = taskJarSubmitted;
	}

	public String getJobStatus() {
		return jobStatus;
	}

	public void setJobStatus(String jobStatus) {
		this.jobStatus = jobStatus;
	}

	public Set<MdcResultStageExecutors> getStageExecutors() {
		return stageExecutors;
	}

	public void setStageExecutors(Set<MdcResultStageExecutors> stageExecutors) {
		this.stageExecutors = stageExecutors;
	}

	public String getJobid() {
		return jobid;
	}

	public void setJobid(String jobid) {
		this.jobid = jobid;
	}
	
	
	
}
