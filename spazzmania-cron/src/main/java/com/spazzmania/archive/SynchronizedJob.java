/**
 * 
 */
package com.spazzmania.archive;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.log4j.FileAppender;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

/**
 * @author tjbillingsley
 * 
 */
public class SynchronizedJob implements Job {
	private Scheduler scheduler;
	private Boolean jobAllowMultipleInstances;
	private SynchronizedJobListener synchronizedJobListener;

	/**
	 * syncJobs holds a set of jobKeys referencing a sub-map of a boolean
	 * "job completed" indicator plus a string designating how to handle a job
	 * exiting on error.
	 */
	private String jobClassName;
	private SynchronizedJobClass jobClass;
	private String[] jobArgs;
	private String jobLogThreadName;
	private String jobLogClassName;
	private String jobLogPattern = "%-5p | %d %t %c: %m%n";
	private String jobLogPath;
	private Boolean initJobLog = false; // Default to append log

	public static String SYNC_JOB_CLASS = "SyncJobClass";
	public static String SYNC_JOB_ALLOW_MULTI = "SyncJobAllowMultipleInstances";
	public static String SYNC_JOB_ARGS = "JobArgs";
	public static String SYNC_JOB_LOG_THREAD_NAME = "JobLogThread";
	public static String SYNC_JOB_LOG_CLASS_NAME = "JobLogClass";
	public static String SYNC_JOB_LOG_PATTERN = "JobLogPattern";
	public static String SYNC_JOB_LOG_PATH = "JobLogFilePath";
	public static String SYNC_JOB_INIT_LOG_FILE = "JobLogInitLogFile";

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.quartz.Job#execute(org.quartz.JobExecutionContext)
	 */
	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		scheduler = context.getScheduler();

		checkJobParameters(context);
		setupJobLogging(context);
		checkMultipleInstances(context);

		synchronizedJobListener = SynchronizedJobListenerFactory.getInstance()
				.createSynchronizedJobListener(context);

		waitForSyncJobs(context);
		executeJob(context);
	}
	
	public void setupJobLogging(JobExecutionContext context) {
		org.apache.log4j.PatternLayout layout = new org.apache.log4j.PatternLayout(jobLogPattern);

		org.apache.log4j.Logger logger = org.apache.log4j.Logger
				.getLogger(jobLogClassName);

		if (initJobLog) {
			FileAppender appender = null;
			try {
				appender = new FileAppender(layout, jobLogPath + jobLogThreadName + ".log",
						initJobLog);
			} catch (IOException e) {
				e.printStackTrace();
			}

			logger.addAppender(appender);
		}
	}

	public void executeJob(JobExecutionContext context) {
		try {
			jobClass = (SynchronizedJobClass) Class.forName(jobClassName)
					.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(
					"Error instantiating and executing SynchronizedJobClass: "
							+ jobClassName + ". " + e.getMessage());
		}

		jobClass.execute(jobArgs);
	}

	/**
	 * Retrieve the following parameters set in the JobDataMap in
	 * JobExecutionContext. <list> <li/><i>SYNCHRONIZED_JOB_CLASS</i>, key
	 * 'SyncJobClass': Required, this is the fully qualified job class that will
	 * be loaded and executed. <li/><i>SYNCHRONIZED_JOB_ARGS</i>, key
	 * 'SyncJobArgs', Optional argument string in the command line format. This
	 * is handed to the job class's execute method as String[] args. </list>
	 * <p/>
	 * &nbsp;
	 * 
	 * @param context
	 *            - JobExecutionContext provided by the quartz scheduler
	 */
	public void checkJobParameters(JobExecutionContext context) {
		JobDataMap dataMap = context.getJobDetail().getJobDataMap();

		if (dataMap.containsKey(SYNC_JOB_CLASS)) {
			jobClassName = (String) dataMap.get(SYNC_JOB_CLASS);
		} else {
			throw new RuntimeException("JobDataMap missing key: "
					+ SYNC_JOB_CLASS);
		}
		
		jobLogThreadName = jobClassName;
		jobLogClassName = jobClassName;

		try {
			if (Class.forName(jobClassName) != null)
				;
		} catch (Exception e) {
			throw new RuntimeException("Error loading SynchronizedJobClass: "
					+ jobClassName);
		}

		if (dataMap.containsKey(SYNC_JOB_ARGS)) {
			String args = (String) dataMap.get(SYNC_JOB_ARGS);
			Pattern argPattern = Pattern.compile("[\\s\\t]+");
			jobArgs = argPattern.split(args);
		} else {
			jobArgs = new String[0];
		}

		if (dataMap.containsKey(SYNC_JOB_ALLOW_MULTI)) {
			jobAllowMultipleInstances = new Boolean(
					(String) dataMap.get(SYNC_JOB_ALLOW_MULTI));
		} else {
			jobAllowMultipleInstances = new Boolean(true);
		}
		
		if (dataMap.containsKey(SYNC_JOB_LOG_THREAD_NAME)) {
			jobLogThreadName = dataMap.getString(SYNC_JOB_LOG_THREAD_NAME);
		}
		
		if (dataMap.containsKey(SYNC_JOB_LOG_CLASS_NAME)) {
			jobLogClassName = dataMap.getString(SYNC_JOB_LOG_CLASS_NAME);
		}
		
		if (dataMap.containsKey(SYNC_JOB_LOG_PATTERN)) {
			jobLogPattern = dataMap.getString(SYNC_JOB_LOG_PATTERN);
		}
		
		if (dataMap.containsKey(SYNC_JOB_LOG_PATH)) {
			jobLogPath = dataMap.getString(SYNC_JOB_LOG_PATH);
			if (!jobLogPath.endsWith("/")) {
				jobLogPath = jobLogPath + "/";
			}
		}
		
		if (dataMap.containsKey(SYNC_JOB_INIT_LOG_FILE)) {
			initJobLog = dataMap.getBooleanValueFromString(SYNC_JOB_INIT_LOG_FILE);
		}
	}
	
	public void checkMultipleInstances(JobExecutionContext context) {
		if (jobAllowMultipleInstances) {
			return;
		}

		try {
			for (JobExecutionContext current : scheduler
					.getCurrentlyExecutingJobs()) {
				if (!context.equals(current)) {
					if (jobClassName == current.getJobDetail().getJobDataMap()
							.getString(SYNC_JOB_CLASS)) {
						throw new RuntimeException(
								"SynchronizedJob - Concurrent classes not allowed: "
										+ jobClassName
										+ " per job configuration");
					}
				}
			}
		} catch (SchedulerException se) {
		}
	}

	public void waitForSyncJobs(JobExecutionContext context) {
		boolean jobsProcessing = true;
		while (jobsProcessing) {
			SyncJobListenerStatus status = synchronizedJobListener.getStatus();
			if (status == SyncJobListenerStatus.JOB_COMPLETED) {
				return;
			} else if (status == SyncJobListenerStatus.JOB_EXITED_ON_ERROR) {
				JobKey jobKey = context.getJobDetail().getKey();
				throw new RuntimeException(
						String.format(
								"Synchronized Job was vetoed: %s/%s, this job aborting",
								jobKey.getGroup(), jobKey.getName()));
			} else if (status == SyncJobListenerStatus.JOB_VETOED) {
				JobKey jobKey = context.getJobDetail().getKey();
				throw new RuntimeException(
						String.format(
								"Synchronized Job exited on error: %s/%s, this job aborting",
								jobKey.getGroup(), jobKey.getName()));
			}
			try {
				Thread.sleep(60000l);
			} catch (InterruptedException e) {
				throw new RuntimeException(
						"Job Interrupted while waiting for other job steps to complete");
			}
		}
	}
}
