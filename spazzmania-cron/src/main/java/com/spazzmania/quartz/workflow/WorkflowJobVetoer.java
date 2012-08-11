/**
 * 
 */
package com.spazzmania.quartz.workflow;

import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.TriggerListener;

/**
 * @author billintj
 *
 */
public class WorkflowJobVetoer implements TriggerListener {
	
	private JobKey jobToVeto;

	public WorkflowJobVetoer(JobKey jobToVeto) {
		this.jobToVeto = jobToVeto;
	}
	
	/* (non-Javadoc)
	 * @see org.quartz.TriggerListener#getName()
	 */
	@Override
	public String getName() {
		return "WorkflowJobVetoer-" + jobToVeto.toString();
	}

	/* (non-Javadoc)
	 * @see org.quartz.TriggerListener#triggerComplete(org.quartz.Trigger, org.quartz.JobExecutionContext, org.quartz.Trigger.CompletedExecutionInstruction)
	 */
	@Override
	public void triggerComplete(Trigger arg0, JobExecutionContext arg1,
			CompletedExecutionInstruction arg2) {
	}

	/* (non-Javadoc)
	 * @see org.quartz.TriggerListener#triggerFired(org.quartz.Trigger, org.quartz.JobExecutionContext)
	 */
	@Override
	public void triggerFired(Trigger arg0, JobExecutionContext arg1) {
	}

	/* (non-Javadoc)
	 * @see org.quartz.TriggerListener#triggerMisfired(org.quartz.Trigger)
	 */
	@Override
	public void triggerMisfired(Trigger arg0) {
	}

	/* (non-Javadoc)
	 * @see org.quartz.TriggerListener#vetoJobExecution(org.quartz.Trigger, org.quartz.JobExecutionContext)
	 */
	@Override
	public boolean vetoJobExecution(Trigger arg0, JobExecutionContext context) {
		if (context.getJobDetail().getKey() == jobToVeto) {
			try {
				context.getScheduler().getListenerManager().removeTriggerListener(getName());
			} catch (SchedulerException e) {
				e.printStackTrace();
			}
			return true;
		}
		return false;
	}

}
