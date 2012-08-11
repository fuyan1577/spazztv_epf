package com.spazzmania.quartz.workflow;

import java.util.HashMap;
import java.util.Map;

import org.quartz.JobKey;

public class WorkflowDependency {

	private JobKey jobKey;
	private WorkflowStatus status = WorkflowStatus.PENDING;
	private Map<WorkflowStatus, WorkflowAction> statusActions = new HashMap<WorkflowStatus, WorkflowAction>();

	public WorkflowDependency() {
	}
	
	public WorkflowDependency(JobKey jobKey) {
		this.jobKey = jobKey;
	}
	
	/**
	 * @return the jobKey
	 */
	public JobKey getJobKey() {
		return jobKey;
	}

	/**
	 * @return the status
	 */
	public WorkflowStatus getStatus() {
		return status;
	}

	/**
	 * @param status
	 *            the status to set
	 */
	public void setStatus(WorkflowStatus status) {
		this.status = status;
	}

	/**
	 * @return the statusAction
	 */
	public WorkflowAction getStatusAction(WorkflowStatus status) {
		return statusActions.get(status);
	}

	/**
	 * @param statusAction
	 *            the statusAction to set
	 */
	public void setStatusAction(WorkflowStatus status, WorkflowAction action) {
		statusActions.put(status, action);
	}

	/**
	 * @return the action
	 */
	public WorkflowAction getAction() {
		// Otherwise, return the default action
		WorkflowAction action = WorkflowAction.TRIGGER_JOB;
		if (statusActions.containsKey(status)) {
			action = statusActions.get(status);
		} else if (status == WorkflowStatus.PENDING) {
			action = WorkflowAction.WAIT;
		} else if (status == WorkflowStatus.IN_PROGRESS) {
			action = WorkflowAction.WAIT;
		} else if (status == WorkflowStatus.VETOED) {
			action = WorkflowAction.EXIT;
		} else if (status == WorkflowStatus.GENERATED_EXCEPTION) {
			action = WorkflowAction.EXIT;
		}
		return action;
	}
}
