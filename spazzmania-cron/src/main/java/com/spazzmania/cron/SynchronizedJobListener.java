/**
 * 
 */
package com.spazzmania.cron;

import java.util.Map;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.SchedulerException;
import org.quartz.impl.matchers.KeyMatcher;

/**
 * @author billintj
 * 
 */
public class SynchronizedJobListener implements JobListener {

	String name;

	SyncJobListenerStatus status;

	Map<JobKey, SyncJobListener> syncJobListeners;
	
	public void setJobListener(JobExecutionContext context, JobKey jobKey, SyncJobListener syncJobListener) {
		syncJobListeners.put(jobKey, syncJobListener);
		try {
			syncJobListeners.put(jobKey, syncJobListener);
			context.getScheduler().getListenerManager().addJobListener(this,
					KeyMatcher.keyEquals(jobKey));
		} catch (SchedulerException se) {
			throw new RuntimeException(se.getMessage());
		}
	}

	/**
	 * @return the status
	 */
	public SyncJobListenerStatus getStatus() {
		return status;
	}

	/**
	 * @param status
	 *            the status to set
	 */
	public void setStatus(SyncJobListenerStatus status) {
		this.status = status;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.quartz.JobListener#getName()
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.quartz.JobListener#jobExecutionVetoed(org.quartz.JobExecutionContext)
	 */
	@Override
	public void jobExecutionVetoed(JobExecutionContext context) {
		SyncJobListener syncJobListener = syncJobListeners.get(context
				.getJobDetail().getKey());

		status = SyncJobListenerStatus.JOB_VETOED;

		if (syncJobListener.getJobErrorHandling() == SyncJobErrorHandling.EXIT_WITH_ERROR_ON_VETO) {
			JobKey jobKey = context.getJobDetail().getKey();

			throw new RuntimeException(String.format(
					"SynchronizedJob - sync job was vetoed: %s/%s",
					jobKey.getGroup(), jobKey.getName()));
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.quartz.JobListener#jobToBeExecuted(org.quartz.JobExecutionContext)
	 */
	@Override
	public void jobToBeExecuted(JobExecutionContext context) {
		// Ignore - synchronization only applies to jobs running at the time of
		// execution
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.quartz.JobListener#jobWasExecuted(org.quartz.JobExecutionContext,
	 * org.quartz.JobExecutionException)
	 */
	@Override
	public void jobWasExecuted(JobExecutionContext context,
			JobExecutionException je) {
		SyncJobListener syncJobListener = syncJobListeners.get(context
				.getJobDetail().getKey());

		if (je != null) {
			if (syncJobListener.getJobErrorHandling() == SyncJobErrorHandling.EXIT_WITH_ERROR_ON_VETO) {
				JobKey jobKey = context.getJobDetail().getKey();

				throw new RuntimeException(
						String.format(
								"SynchronizedJob - sync job exited with error: %s/%s, \"%s\"",
								jobKey.getGroup(), jobKey.getName(),
								je.getMessage()));
			}
		}

		if (syncJobListener.getJobErrorHandling() == SyncJobErrorHandling.EXIT_QUIETLY_ON_VETO) {
			setStatus(SyncJobListenerStatus.JOB_VETOED);
		}
	}
}
