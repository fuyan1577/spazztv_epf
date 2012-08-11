/**
 * 
 */
package com.spazzmania.quartz.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.matchers.EverythingMatcher;
import org.slf4j.Logger;

/**
 * @author billintj
 * 
 */
public class WorkflowTask implements JobListener {

	private String name;
	
	private JobDetail jobDetail;

	private Map<JobKey, WorkflowDependency> dependencies = new HashMap<JobKey, WorkflowDependency>();

	private JobKey jobKey;

	private Scheduler scheduler;

	private Logger logger;
	
	private boolean jobActionExecuted = false;

	public WorkflowTask() {
	}

//	public WorkflowTask(JobKey jobKey) {
//		this.jobKey = jobKey;
//		this.name = "WorkflowTask-" + jobKey.toString();
//	}

	public WorkflowTask(JobDetail jobDetail) {
		this.jobDetail = jobDetail;
		this.jobKey = jobDetail.getKey();
		this.name = "WorkflowTask-" + jobDetail.getKey().toString();
	}

	public JobKey getKey() {
		return jobKey;
	}

	public void addDependency(JobKey jobKey) {
		if (!dependencies.containsKey(jobKey)) {
			dependencies.put(jobKey, new WorkflowDependency(jobKey));
		}
	}

	public void addDependency(JobKey jobKey, WorkflowStatus status,
			WorkflowAction action) {
		addDependency(jobKey);
		dependencies.get(jobKey).setStatusAction(status, action);
	}
	
	public void setDependencies(List<WorkflowDependency> dependencies) {
		for (WorkflowDependency dependency : dependencies) {
			this.dependencies.put(dependency.getJobKey(), dependency);
		}
	}

	public void setDependencyStatus(JobKey jobKey, WorkflowStatus status) {
		dependencies.get(jobKey).setStatus(status);
	}

	public WorkflowStatus getDependencyStatus(JobKey jobKey) {
		return dependencies.get(jobKey).getStatus();
	}

	public Map<JobKey, WorkflowDependency> getDependencies() {
		return dependencies;
	}

	/**
	 * @return the logger
	 */
	public Logger getLogger() {
		return logger;
	}

	/**
	 * @param logger
	 *            the logger to set
	 */
	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	/**
	 * @return the scheduler
	 */
	public Scheduler getScheduler() {
		return scheduler;
	}

	/**
	 * @param scheduler
	 *            the scheduler to set
	 */
	public void setScheduler(Scheduler scheduler) {
		this.scheduler = scheduler;
		try {
			scheduler.getListenerManager().addJobListener(this,
					EverythingMatcher.allJobs());
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.quartz.JobListener#getName()
	 */
	public String getName() {
		return name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.quartz.JobListener#jobExecutionVetoed(org.quartz.JobExecutionContext)
	 */
	public void jobExecutionVetoed(JobExecutionContext context) {
		JobKey vJobKey = context.getJobDetail().getKey();
		if (dependencies.containsKey(vJobKey)) {
			dependencies.get(vJobKey).setStatus(WorkflowStatus.VETOED);
			logDependencyStatus(vJobKey, WorkflowStatus.VETOED);
			processDependencies();
		} else if (vJobKey == jobKey) {
			logDependencyStatus(vJobKey, WorkflowStatus.VETOED);
			exit();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.quartz.JobListener#jobToBeExecuted(org.quartz.JobExecutionContext)
	 */
	public void jobToBeExecuted(JobExecutionContext context) {
		JobKey vJobKey = context.getJobDetail().getKey();
		if (dependencies.containsKey(vJobKey)) {
			dependencies.get(vJobKey).setStatus(WorkflowStatus.IN_PROGRESS);
			logDependencyStatus(vJobKey, WorkflowStatus.IN_PROGRESS);
			processDependencies();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.quartz.JobListener#jobWasExecuted(org.quartz.JobExecutionContext,
	 * org.quartz.JobExecutionException)
	 */
	public void jobWasExecuted(JobExecutionContext context,
			JobExecutionException exception) {
		JobKey vJobKey = context.getJobDetail().getKey();
		if (dependencies.containsKey(vJobKey)) {
			if (exception == null) {
				dependencies.get(vJobKey).setStatus(WorkflowStatus.COMPLETED);
				logDependencyStatus(vJobKey, WorkflowStatus.COMPLETED);
			} else {
				dependencies.get(vJobKey).setStatus(
						WorkflowStatus.GENERATED_EXCEPTION);
				logDependencyStatus(vJobKey, WorkflowStatus.GENERATED_EXCEPTION);
			}
			processDependencies();
		} else if (vJobKey == jobKey) {
			exit();
		}
	}

	public void processDependencies() {
		//Exit early if the job for this WorkflowTask has already executed
		if (jobActionExecuted) {
			return;
		}
		
		WorkflowAction action = WorkflowAction.TRIGGER_JOB;
		// To continue WAIT, at least one action must be WAIT
		// To continue TRIGGER_JOB, all actions must be TRIGGER_JOB
		// Any action other than WAIT and TRIGGER_JOB is the action taken
		// If any dependency action is other than TRIGGER_JOB, that is the
		// action
		// taken
		for (JobKey jobKey : dependencies.keySet()) {
			WorkflowAction depAction = dependencies.get(jobKey).getAction();

			if ((depAction != WorkflowAction.WAIT)
					&& (depAction != WorkflowAction.TRIGGER_JOB)) {
				action = depAction;
				break;
			} else if (depAction == WorkflowAction.WAIT) {
				action = depAction;
				break;
			}
		}

		if (action == WorkflowAction.TRIGGER_JOB) {
			jobActionExecuted = true;
			triggerJob();
		} else if (action == WorkflowAction.EXIT) {
			jobActionExecuted = true;
			vetoJob();
		} else {
			// Do nothing - more dependencies to go
		}
	}

	public void exit() {
		try {
			scheduler.deleteJob(jobKey);
			scheduler.getListenerManager().removeJobListener(getName());
		} catch (SchedulerException e) {
			logger.error(String
					.format("Listener %s - scheduler returned error when removing WorkflowDendency listener: %s",
							getName(), e.getMessage()));
		}
	}

	public void triggerJob() {
		try {
			scheduler.triggerJob(jobKey);
		} catch (SchedulerException e) {
			logger.error(String
					.format("Job %s - scheduler returned error when triggering job: %s",
							jobKey, e.getMessage()));
		}
	}

	/**
	 * <p>
	 * Vetoes this job via the scheduler...
	 * <p>
	 * Adds a WorkflowJobVetoer to the scheduler and then triggers the main job.
	 * The WorkflowJobVetoer signals the scheduler that the job should be
	 * vetoed.
	 * <p>
	 * This allows veto event notifications to be sent by the scheduler to all
	 * other WorkflowTasks dependent on this job.
	 */
	public void vetoJob() {
		try {
			scheduler.getListenerManager().addTriggerListener(
					new WorkflowJobVetoer(jobKey));
			triggerJob();
		} catch (SchedulerException e) {
			logger.error(String
					.format("Job %s - scheduler returned error when vetoeing job: %s",
							jobKey, e.getMessage()));
		}
	}

	public void logDependencyStatus(JobKey dJobKey, WorkflowStatus depStatus) {
		logger.info(String.format("Job %s - returned the status: %s", dJobKey,
				depStatus.toString()));
	}
}
