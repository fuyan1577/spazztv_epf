package com.spazzmania.quartz.workflow;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;

public class WorkflowDispatcher implements JobListener, Job {
	
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		// TODO Auto-generated method stub

	}

	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	public void jobExecutionVetoed(JobExecutionContext arg0) {
		// TODO Auto-generated method stub

	}

	public void jobToBeExecuted(JobExecutionContext arg0) {
		// TODO Auto-generated method stub

	}

	public void jobWasExecuted(JobExecutionContext arg0,
			JobExecutionException arg1) {
		// TODO Auto-generated method stub

	}

}
