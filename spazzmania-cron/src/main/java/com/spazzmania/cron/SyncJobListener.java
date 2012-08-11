package com.spazzmania.cron;

import org.quartz.JobKey;

/**
 * @author billintj
 *
 */
public class SyncJobListener {
	private JobKey jobKey;
	private SyncJobErrorHandling jobErrorHandling;
	private boolean jobCompleted;
	
	public SyncJobListener(JobKey jobKey, SyncJobErrorHandling jobErrorHandling) {
		this.jobKey = jobKey;
		this.jobErrorHandling = jobErrorHandling;
		this.jobCompleted = false;
	}
	
	/**
	 * @return the jobKey
	 */
	public JobKey getJobKey() {
		return jobKey;
	}

	/**
	 * @param jobKey the jobKey to set
	 */
	public void setJobKey(JobKey jobKey) {
		this.jobKey = jobKey;
	}

	/**
	 * @return the jobErrorHandling
	 */
	public SyncJobErrorHandling getJobErrorHandling() {
		return jobErrorHandling;
	}

	/**
	 * @param jobErrorHandling the jobErrorHandling to set
	 */
	public void setJobErrorHandling(SyncJobErrorHandling jobErrorHandling) {
		this.jobErrorHandling = jobErrorHandling;
	}

	/**
	 * @return the jobCompleted
	 */
	public boolean isJobCompleted() {
		return jobCompleted;
	}

	/**
	 * @param jobCompleted the jobCompleted to set
	 */
	public void setJobCompleted(boolean jobCompleted) {
		this.jobCompleted = jobCompleted;
	}

}
