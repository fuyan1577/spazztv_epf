/**
 * 
 */
package com.spazzmania.archive;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.impl.matchers.GroupMatcher;

/**
 * @author billintj
 *
 */
public class SynchronizedJobListenerFactory {

	private static SynchronizedJobListenerFactory synchronizedJobListenerFactory;
	
	private SynchronizedJobListenerFactory() {
	}
	
	public static SynchronizedJobListenerFactory getInstance() {
		if (synchronizedJobListenerFactory == null) {
			synchronizedJobListenerFactory = new SynchronizedJobListenerFactory();
		}
		return synchronizedJobListenerFactory;
	}
	
	public SynchronizedJobListener createSynchronizedJobListener(JobExecutionContext context)	{
		SynchronizedJobListener synchronizedJobListener = new SynchronizedJobListener();
		
		Map<String, SyncJobListenerParm> jobListenerParms = loadSyncJobListenerParms(context);
		String group, name;
		
		for (String parmKey : jobListenerParms.keySet()) {
			SyncJobListenerParm syncJobParm = jobListenerParms.get(parmKey);

			group = syncJobParm.getJobGroup();
			name = syncJobParm.getJobName();
			SyncJobErrorHandling jobErrorHandling = syncJobParm.getJobErrorHandling();

			if (group != null && name != null) {
				JobKey jobKey = new JobKey(group,name);
				setSyncWaitForJob(synchronizedJobListener, context, jobKey, jobErrorHandling);
			} else if (group != null) {
				setSyncGroupAllJobs(synchronizedJobListener, context, group, jobErrorHandling);
			} else if (name != null) {
				setSyncNameAllJobs(synchronizedJobListener, context, name, jobErrorHandling);
			}
		}
		
		return synchronizedJobListener;
	}

	public void setSyncWaitForJob(SynchronizedJobListener synchronizedJobListener, JobExecutionContext context, JobKey jobKey, SyncJobErrorHandling jobErrorHandling) {
		SyncJobListener syncJobListener = new SyncJobListener(jobKey, jobErrorHandling);
		synchronizedJobListener.setJobListener(context, jobKey, syncJobListener);
	}
	
	public void setSyncGroupAllJobs(SynchronizedJobListener synchronizedJobListener, JobExecutionContext context, String jobGroup, SyncJobErrorHandling jobErrorHandling) {
		try {
			Set<JobKey> jobKeys = context.getScheduler().getJobKeys(GroupMatcher
					.jobGroupEquals(jobGroup));
			for (JobKey jobKey : jobKeys) {
				setSyncWaitForJob(synchronizedJobListener, context, jobKey, jobErrorHandling);
			}
		} catch (SchedulerException se) {
			throw new RuntimeException(se.getMessage());
		}
	}

	public void setSyncNameAllJobs(SynchronizedJobListener synchronizedJobListener, JobExecutionContext context, String jobName, SyncJobErrorHandling jobErrorHandling) {
		try {
			for (String group : context.getScheduler().getJobGroupNames()) {
				Set<JobKey> jobKeys = context.getScheduler().getJobKeys(GroupMatcher
						.jobGroupEquals(group));
				for (JobKey jobKey : jobKeys) {
					if (jobKey.getName() == jobName) {
						setSyncWaitForJob(synchronizedJobListener, context, jobKey, jobErrorHandling);
					}
				}
			}
		} catch (SchedulerException se) {
			throw new RuntimeException(se.getMessage());
		}

	}
	
	public Map<String, SyncJobListenerParm> loadSyncJobListenerParms(JobExecutionContext context) {
		JobDataMap dataMap = context.getJobDetail().getJobDataMap();
		
		String parmKey;
		String group;
		String name;
		String[] groupAndName;
		Map<String,SyncJobListenerParm> syncJobParms = new HashMap<String,SyncJobListenerParm>();

		for (String key : dataMap.keySet()) {
			// Check for "SyncWaitForGroup" keys
			if (key == SyncJobListenerParm.SYNC_WAIT_JOB_GROUP) {
				parmKey = SyncJobListenerParm.SYNC_JOB_PARM_KEY_DEFAULT;
				group = dataMap.getString(key);
				setJobParmKeyValue(syncJobParms,parmKey,
						SyncJobListenerParm.SYNC_JOB_PARM_KEY_GROUP, group);
			} else if (key == SyncJobListenerParm.SYNC_WAIT_JOB_NAME) {
				parmKey = SyncJobListenerParm.SYNC_JOB_PARM_KEY_DEFAULT;
				name = dataMap.getString(key);
				setJobParmKeyValue(syncJobParms,parmKey, SyncJobListenerParm.SYNC_JOB_PARM_KEY_JOB,
						name);
			} else if (key.startsWith(SyncJobListenerParm.SYNC_WAIT_JOB_GROUP)) {
				parmKey = key.replaceFirst(SyncJobListenerParm.SYNC_WAIT_JOB_GROUP, "");
				group = dataMap.getString(key);
				setJobParmKeyValue(syncJobParms,parmKey,
						SyncJobListenerParm.SYNC_JOB_PARM_KEY_GROUP, group);
			} else if (key.startsWith(SyncJobListenerParm.SYNC_WAIT_JOB_NAME)) {
				parmKey = key.replaceFirst(SyncJobListenerParm.SYNC_WAIT_JOB_NAME, "");
				name = dataMap.getString(key);
				// If the name is a group and name "mygroup/myname", split them
				if (name.matches("[,/:;]")) {
					groupAndName = name.split("[,;/]");
					group = groupAndName[0];
					name = groupAndName[1];
					setJobParmKeyValue(syncJobParms,parmKey,
							SyncJobListenerParm.SYNC_JOB_PARM_KEY_GROUP, group);
				}

				setJobParmKeyValue(syncJobParms,parmKey, SyncJobListenerParm.SYNC_JOB_PARM_KEY_JOB,
						name);
			}
		}
		
		return syncJobParms;
	} 
	
	public void setJobParmKeyValue(Map<String,SyncJobListenerParm> syncJobParms, String parmKey, String subKey, String value) {
		SyncJobListenerParm jobParm;

		if (!syncJobParms.containsKey(parmKey)) {
			jobParm = new SyncJobListenerParm();
			jobParm.setJobErrorHandling(SyncJobListenerParm.SYNC_JOB_EXIT_ERROR_ON_ERROR);
		} else {
			jobParm = syncJobParms.get(parmKey);
		}
		
		if (subKey == SyncJobListenerParm.SYNC_JOB_PARM_KEY_GROUP) {
			jobParm.setJobGroup(value);
		} else if (subKey == SyncJobListenerParm.SYNC_JOB_PARM_KEY_JOB) {
			if (value.matches("[,/:;]")) {
				String[] groupAndName = value.split("[,;/]");
				jobParm.setJobGroup(groupAndName[0]);
				jobParm.setJobName(groupAndName[1]);
			}
		} else if (subKey == SyncJobListenerParm.SYNC_JOB_PARM_KEY_ERROR_HANDLING) {
			jobParm.setJobErrorHandling(value);
		}

		syncJobParms.put(parmKey, jobParm);
	}
}
