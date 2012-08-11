/**
 * 
 */
package com.spazzmania.misc;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * @author billintj
 *
 */
public class TestJob implements Job {
	/* (non-Javadoc)
	 * @see org.quartz.Job#execute(org.quartz.JobExecutionContext)
	 */
	@Override
	public void execute(JobExecutionContext context) throws JobExecutionException {
		System.out.println("TestJob-" + context.getJobDetail().getKey() + " Starting");
		System.out.println("TestJob-" + context.getJobDetail().getKey() + " Completed");
	}
}
