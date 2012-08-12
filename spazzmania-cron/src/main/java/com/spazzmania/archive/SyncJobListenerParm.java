package com.spazzmania.archive;

public class SyncJobListenerParm {
	private String jobGroup;
	private String jobName;
	private String jobErrorHandling;

	public static String SYNC_JOB_PARM_KEY_DEFAULT = "DEFAULT";
	public static String SYNC_JOB_PARM_KEY_GROUP = "Group";
	public static String SYNC_JOB_PARM_KEY_JOB = "Job";
	public static String SYNC_JOB_PARM_KEY_ERROR_HANDLING = "ErrorHandling";

	public static String SYNC_JOB_GROUP_DEFAULT = "DEFAULT";
	public static String SYNC_JOB_IGNORE_ERROR = "IgnoreError";
	public static String SYNC_JOB_EXIT_ERROR_ON_ERROR = "ExitOnError";
	public static String SYNC_JOB_EXIT_QUIETLY_ON_ERROR = "ExitWithError";
	public static String SYNC_JOB_EXIT_ERROR_ON_VETO = "ExitOnError";
	public static String SYNC_JOB_EXIT_QUIETLY_ON_VETO = "ExitWithError";

	public static String SYNC_WAIT_JOB_NAME = "SyncWaitForName";
	public static String SYNC_WAIT_JOB_GROUP = "SyncWaitForGroup";
	public static String SYNC_WAIT_ERROR_HANDLING = "SyncWaitJobErrorHandling";
	public static String SYNC_WAIT_IGNORE_ERROR = "IgnoreError";
	public static String SYNC_WAIT_EXIT_ON_ERROR = "ExitOnError";
	public static String SYNC_WAIT_EXIT_WITH_ERROR = "ExitWithError";

	public SyncJobListenerParm() {
	}

	public SyncJobListenerParm(String jobGroup, String jobName,
			String jobErrorHandling) {
		this.jobGroup = jobGroup;
		this.jobName = jobName;
		this.jobErrorHandling = jobErrorHandling;

		if (jobGroup == null) {
			this.jobGroup = SYNC_JOB_GROUP_DEFAULT;
		}
	}

	public SyncJobErrorHandling getJobErrorHandling() {
		if (jobErrorHandling == SYNC_JOB_IGNORE_ERROR) {
			return SyncJobErrorHandling.IGNORE_ERROR;
		} else if (jobErrorHandling == SYNC_JOB_EXIT_ERROR_ON_ERROR) {
			return SyncJobErrorHandling.EXIT_WITH_ERROR_ON_ERROR;
		} else if (jobErrorHandling == SYNC_JOB_EXIT_QUIETLY_ON_ERROR) {
			return SyncJobErrorHandling.EXIT_QUIETLY_ON_ERROR;
		} else if (jobErrorHandling == SYNC_JOB_EXIT_ERROR_ON_VETO) {
			return SyncJobErrorHandling.EXIT_WITH_ERROR_ON_VETO;
		} else if (jobErrorHandling == SYNC_JOB_EXIT_QUIETLY_ON_VETO) {
			return SyncJobErrorHandling.EXIT_QUIETLY_ON_VETO;
		}
		return SyncJobErrorHandling.EXIT_WITH_ERROR_ON_ERROR;
	}

	public void setJobErrorHandling(String jobErrorHandling) {
		this.jobErrorHandling = jobErrorHandling;
	}

	public String getJobGroup() {
		return jobGroup;
	}

	public void setJobGroup(String jobGroup) {
		this.jobGroup = jobGroup;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
}
