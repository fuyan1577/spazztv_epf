/**
 * 
 */
package com.spazzmania.quartz.workflow;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobKey;

/**
 * @author billintj
 *
 */
public class WorkflowJob  {
	private String description;
	private Class<? extends Job> jobClass;
	private JobDataMap jobDataMap;
	private JobKey jobKey;
	
	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}
	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}
	/**
	 * @return the jobClass
	 */
	public Class<? extends Job> getJobClass() {
		return jobClass;
	}
	/**
	 * @param jobClass the jobClass to set
	 */
	public void setJobClass(Class<? extends Job> jobClass) {
		this.jobClass = jobClass;
	}
	/**
	 * @return the jobDataMap
	 */
	public JobDataMap getJobDataMap() {
		return jobDataMap;
	}
	/**
	 * @param jobDataMap the jobDataMap to set
	 */
	public void setJobDataMap(JobDataMap jobDataMap) {
		this.jobDataMap = jobDataMap;
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
	
}
