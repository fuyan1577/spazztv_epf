/**
 * 
 */
package com.spazzmania.archive;

import static org.quartz.impl.matchers.AndMatcher.and;
import static org.quartz.impl.matchers.GroupMatcher.jobGroupEquals;
import static org.quartz.impl.matchers.NameMatcher.jobNameEquals;

import java.util.HashMap;
import java.util.Map;

import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

/**
 * @author tjbillingsley
 * 
 */
public abstract class JobStep implements Job, JobListener {
	private Scheduler scheduler;
	private String name;
	private Map<String, Boolean> syncGroups = new HashMap<String, Boolean>();
	private Map<String, Boolean> syncJobs = new HashMap<String, Boolean>();
	private Map<String, Map<String, Boolean>> syncGroupJobs = new HashMap<String, Map<String, Boolean>>();

	public static String JOB_GROUP_PREFIX = "WaitForGroup";
	public static String JOB_NAME_PREFIX = "WaitForJob";
	public static Boolean JOB_STEP_NOTIFICATION_LISTENING = Boolean.FALSE;
	public static Boolean JOB_STEP_NOTIFICATION_RECEIVED = Boolean.TRUE;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.quartz.JobListener#getName()
	 */
	@Override
	public String getName() {
		return name;
	}

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
		// TODO Auto-generated method stub
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.quartz.JobListener#jobToBeExecuted(org.quartz.JobExecutionContext)
	 */
	@Override
	public void jobToBeExecuted(JobExecutionContext context) {
		// TODO Auto-generated method stub
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
			JobExecutionException arg1) {
		// TODO Auto-generated method stub
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.quartz.Job#execute(org.quartz.JobExecutionContext)
	 */
	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		scheduler = context.getScheduler();

		setSynchronizedStepListeners(context);
	}

	private void waitForSyncJobs(JobExecutionContext context) {
		Boolean moreJobs = true;
		while (moreJobs) {
			
			//TODO Add synchronization code here

			try {
				Thread.sleep(60000l);
			} catch (InterruptedException e) {
				throw new RuntimeException(
						"Job Interrupted while waiting for other job steps to complete");
			}
		}
	}

	private void setSynchronizedStepListeners(JobExecutionContext context) {
		JobDataMap dataMap = context.getJobDetail().getJobDataMap();
		String group, name;
		String[] groupAndName;

		for (String key : dataMap.keySet()) {
			// Check for "WaitForGroup" keys
			if (key.startsWith(JOB_GROUP_PREFIX)) {
				group = dataMap.getString(key);
				setSyncGroup(group);
			}
			// Check for "WaitForJob" keys
			if (key.startsWith(JOB_NAME_PREFIX)) {
				name = dataMap.getString(key);
				// If the name is a group and name "mygroup,myname", split them
				if (!name.matches("[,;]")) {
					setSyncName(name);
				} else {
					groupAndName = name.split("[,;]");
					setSyncGroupAndName(groupAndName[0], groupAndName[1]);
				}
			}
		}
	}

	private void setSyncGroup(String group) {
		try {
			syncGroups.put(group, JOB_STEP_NOTIFICATION_LISTENING);
			scheduler.getListenerManager().addJobListener(this,
					jobGroupEquals(group));
		} catch (SchedulerException exception) {
			String msg = String.format("Invalid Group Parameter: %s", group);
			throw new RuntimeException(msg);
		}
	}

	private void setSyncGroupAndName(String group, String name) {
		try {
			if (!syncGroupJobs.containsKey(group)) {
				syncGroupJobs.put(group, new HashMap<String, Boolean>());
			}
			HashMap<String, Boolean> jobs = (HashMap<String, Boolean>) syncGroupJobs
					.get(group);
			jobs.put(name, JOB_STEP_NOTIFICATION_LISTENING);
			syncGroupJobs.put(group, jobs);
			scheduler.getListenerManager().addJobListener(this,
					and(jobGroupEquals(group), jobNameEquals(name)));
		} catch (SchedulerException exception) {
			String msg = String.format("Invalid Group Parameter: %s : %s",
					group, name);
			throw new RuntimeException(msg);
		}
	}

	private void setSyncName(String name) {
		try {
			syncJobs.put(name, JOB_STEP_NOTIFICATION_LISTENING);
			scheduler.getListenerManager().addJobListener(this,
					jobNameEquals(name));
		} catch (SchedulerException exception) {
			String msg = String.format("Invalid Group Parameter: %s", name);
			throw new RuntimeException(msg);
		}
	}
}
