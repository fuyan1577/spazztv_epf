/**
 * 
 */
package com.spazzmania.cron;

import org.kohsuke.args4j.Option;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * @author Thomas Billingsley
 * 
 */
public class ShellExecJob implements Job {
	public static String SHELL_EXECUTABLE = "executable";
	public static String SHELL_PARAMETERS = "parameters";
	public static String SHELL_DIRECTORY = "directory";

	@Option(name = "--parameters", metaVar = "<parameters>", usage = "parameter arguments enclosed by \"\"")
	private String parameters;

	@Option(name = "--executable", metaVar = "<executable>", usage = "executable script or binary")
	private String executable;

	@Option(name = "--directory", metaVar = "<directory>", usage = "base directory for execution")
	private String directory;

	/**
	 * @return the parameters
	 */
	public String getParameters() {
		return parameters;
	}

	/**
	 * @return the executable
	 */
	public String getExecutable() {
		return executable;
	}

	/**
	 * @return the directory
	 */
	public String getDirectory() {
		return directory;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.quartz.Job#execute(org.quartz.JobExecutionContext)
	 */
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		// TODO Auto-generated method stub
	}

	/**
	 * Loads and verifies the job settings set in the Quartz Configuration for
	 * this job.
	 * 
	 * @param context
	 *            - JobExectionContext (Quartz Scheduler)
	 */
	public void verifyJobDataMap(JobExecutionContext context) {
		JobDataMap dataMap = context.getJobDetail().getJobDataMap();
		if ((!dataMap.containsKey(SHELL_EXECUTABLE))
				|| (!dataMap.containsKey(SHELL_PARAMETERS))
				|| (!dataMap.containsKey(SHELL_DIRECTORY))) {
			throw new RuntimeException(
					"Invalid JobDataMap 'executable', 'parameters' and 'directory' must be provided");
		}
		executable = dataMap.getString(SHELL_EXECUTABLE);
		parameters = dataMap.getString(SHELL_PARAMETERS);
		directory = dataMap.getString(SHELL_DIRECTORY);
	}

	public void parseArgs(String[] args) {
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ShellExecJob job = new ShellExecJob();
		job.parseArgs(args);
		try {
			job.execute(null);
		} catch (JobExecutionException e) {
			throw new RuntimeException(e);
		}
	}

}
