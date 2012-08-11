/**
 * 
 */
package com.spazzmania.quartz.workflow;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.ListenerManager;
import org.quartz.Scheduler;
import org.slf4j.Logger;

/**
 * @author billintj
 * 
 */
public class WorkflowDependencyTest {

	private JobKey jobKey = new JobKey("mainJob/mainGroup");
	private JobKey depJobKey1 = new JobKey("depJob1/mainGroup");
	private JobKey depJobKey2 = new JobKey("depJob2/mainGroup");
	private JobKey depJobKey3 = new JobKey("depJob3/mainGroup");
	private JobKey depJobKey4 = new JobKey("depJob4/mainGroup");

	private WorkflowTask workflowDependency;
	private Scheduler scheduler;
	private Logger logger;
	private ListenerManager listenerManager;

	private JobExecutionContext context1;
	private JobExecutionContext context2;
	private JobExecutionContext context3;
	private JobExecutionContext context4;

	private JobDetail jobDetail;
	private JobDetail jobDetail1;
	private JobDetail jobDetail2;
	private JobDetail jobDetail3;
	private JobDetail jobDetail4;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		logger = EasyMock.createMock(Logger.class);
		listenerManager = EasyMock.createMock(ListenerManager.class);

		context1 = EasyMock.createMock(JobExecutionContext.class);
		context2 = EasyMock.createMock(JobExecutionContext.class);
		context3 = EasyMock.createMock(JobExecutionContext.class);
		context4 = EasyMock.createMock(JobExecutionContext.class);

		jobDetail = EasyMock.createMock(JobDetail.class);
		jobDetail1 = EasyMock.createMock(JobDetail.class);
		jobDetail2 = EasyMock.createMock(JobDetail.class);
		jobDetail3 = EasyMock.createMock(JobDetail.class);
		jobDetail4 = EasyMock.createMock(JobDetail.class);

		// Having trouble with EasyMock on Scheduler.class
		// Using a dummy scheduler
		scheduler = new MockScheduler(listenerManager);

		jobDetail.getKey();
		EasyMock.expectLastCall().andReturn(jobKey).anyTimes();
		EasyMock.replay(jobDetail);

		workflowDependency = new WorkflowTask(jobDetail);
		workflowDependency.setLogger(logger);
		workflowDependency.setScheduler(scheduler);
	}

	/**
	 * Test method for
	 * {@link com.spazzmania.quartz.workflow.WorkflowTask#addDependency(org.quartz.JobKey)}
	 * .
	 */
	@Test
	public void testDefaultJobExecutionVetoed() {
		// Setup the Mocks
		workflowDependency.addDependency(depJobKey1);
		workflowDependency.addDependency(depJobKey2);
		workflowDependency.addDependency(depJobKey3);
		workflowDependency.addDependency(depJobKey4);

		// EasyMock.expect(listenerManager.removeJobListener(workflowDependency.getName())).times(1).andReturn(Boolean.TRUE);
		listenerManager.removeJobListener(workflowDependency.getName());
		EasyMock.expectLastCall().andReturn(Boolean.TRUE).times(1);

		listenerManager.addTriggerListener(EasyMock.anyObject(WorkflowJobVetoer.class));
		EasyMock.expectLastCall().times(1);

		logger.info((String) EasyMock.anyObject());
		EasyMock.expectLastCall().times(1);

		context1.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail1).anyTimes();

		jobDetail1.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey1).times(1);
		
		// EasyMock.replay(scheduler);
		EasyMock.replay(logger);
		EasyMock.replay(listenerManager);
		EasyMock.replay(context1);
		EasyMock.replay(jobDetail1);

		// try {
		// ListenerManager l = scheduler.getListenerManager();
		// } catch (SchedulerException e) {
		// }

		// Execute the method
		workflowDependency.jobExecutionVetoed(context1);

		// EasyMock.verify(scheduler);
		EasyMock.verify(logger);
		EasyMock.verify(context1);
		EasyMock.verify(jobDetail1);
	}

	/**
	 * Test method for
	 * {@link com.spazzmania.quartz.workflow.WorkflowTask#addDependency(org.quartz.JobKey)}
	 */

	@Test
	public void testDefaultJobExecutionVetoed2() { // Setup the Mocks
		workflowDependency.addDependency(depJobKey1);
		workflowDependency.addDependency(depJobKey2);
		workflowDependency.addDependency(depJobKey3);
		workflowDependency.addDependency(depJobKey4);

		listenerManager.removeJobListener(workflowDependency.getName());
		EasyMock.expectLastCall().andReturn(Boolean.TRUE).times(1);

		listenerManager.addTriggerListener(EasyMock.anyObject(WorkflowJobVetoer.class));
		EasyMock.expectLastCall().times(1);

		logger.info((String) EasyMock.anyObject());
		EasyMock.expectLastCall().times(4);

		context1.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail1).anyTimes();
		context2.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail2).anyTimes();
		context3.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail3).anyTimes();
		context4.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail4).anyTimes();

		jobDetail1.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey1).times(1);
		jobDetail2.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey2).times(1);
		jobDetail3.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey3).times(1);
		jobDetail4.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey4).times(1);

		// EasyMock.replay(scheduler);
		EasyMock.replay(logger);
		EasyMock.replay(listenerManager);
		EasyMock.replay(context1);
		EasyMock.replay(context2);
		EasyMock.replay(context3);
		EasyMock.replay(context4);
		EasyMock.replay(jobDetail1);
		EasyMock.replay(jobDetail2);
		EasyMock.replay(jobDetail3);
		EasyMock.replay(jobDetail4);

		// Execute the method
		workflowDependency.jobWasExecuted(context1, null);
		workflowDependency.jobWasExecuted(context2, null);
		workflowDependency.jobWasExecuted(context3, null);
		workflowDependency.jobExecutionVetoed(context4);

		// Verify the results EasyMock.verify(scheduler);
		EasyMock.verify(logger);
		EasyMock.verify(context1);
		EasyMock.verify(context2);
		EasyMock.verify(context3);
		EasyMock.verify(context4);
		EasyMock.verify(jobDetail1);
		EasyMock.verify(jobDetail2);
		EasyMock.verify(jobDetail3);
		EasyMock.verify(jobDetail4);
	}

	/**
	 * Test method for
	 * {@link com.spazzmania.quartz.workflow.WorkflowTask#addDependency(org.quartz.JobKey)}
	 */

	@Test
	public void testDefaultJobExecutionVetoed3() { // Setup the Mocks
		workflowDependency.addDependency(depJobKey1);
		workflowDependency.addDependency(depJobKey2);
		workflowDependency.addDependency(depJobKey3);
		workflowDependency.addDependency(depJobKey4);

		try {
			EasyMock.expect(scheduler.getListenerManager()).andStubReturn(
					listenerManager);
		} catch (Exception e) {
		}

		listenerManager.removeJobListener(workflowDependency.getName());
		EasyMock.expectLastCall().andReturn(Boolean.TRUE).times(1);

		logger.info((String) EasyMock.anyObject());
		EasyMock.expectLastCall().times(2);

		context1.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail1).anyTimes();
		context2.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail2).anyTimes();
		context3.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail3).anyTimes();
		context4.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail4).anyTimes();

		jobDetail1.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey1).times(1);
		jobDetail2.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey2).times(1);
		jobDetail3.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey3).anyTimes();
		jobDetail4.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey4).anyTimes();

		EasyMock.replay(logger);
		EasyMock.replay(listenerManager);
		EasyMock.replay(context1);
		EasyMock.replay(context2);
		EasyMock.replay(context3);
		EasyMock.replay(context4);
		EasyMock.replay(jobDetail1);
		EasyMock.replay(jobDetail2);
		EasyMock.replay(jobDetail3);
		EasyMock.replay(jobDetail4);

		// Execute the method
		workflowDependency.jobWasExecuted(context1, null);
		workflowDependency.jobExecutionVetoed(context2);

		// Verify the results EasyMock.verify(scheduler);
		EasyMock.verify(logger);
		EasyMock.verify(context1);
		EasyMock.verify(context2);
		EasyMock.verify(jobDetail1);
		EasyMock.verify(jobDetail2);
	}

	/*
	 * *
	 * Test method for {@link com.spazzmania.quartz.workflow.WorkflowDependency
	 * #addDependency(org.quartz.JobKey)} .
	 */

	@Test
	public void testDefaultJobWasExecuted() { // Setup the Mocks
		workflowDependency.addDependency(depJobKey1);
		workflowDependency.addDependency(depJobKey2);
		workflowDependency.addDependency(depJobKey3);
		workflowDependency.addDependency(depJobKey4);

		try {
			scheduler.triggerJob(workflowDependency.getKey());
			EasyMock.expectLastCall().times(1);
			EasyMock.expect(scheduler.getListenerManager()).andStubReturn(
					listenerManager);
		} catch (Exception e) {
		}

		// EasyMock.expect(listenerManager.removeJobListener(
		// workflowDependency.getName())).times(1).andReturn(Boolean.TRUE);
		listenerManager.removeJobListener(workflowDependency.getName());
		EasyMock.expectLastCall().andReturn(Boolean.TRUE).times(1);

		logger.info((String) EasyMock.anyObject());
		EasyMock.expectLastCall().times(4);

		context1.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail1).anyTimes();
		context2.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail2).anyTimes();
		context3.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail3).anyTimes();
		context4.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail4).anyTimes();

		jobDetail1.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey1).times(1);
		jobDetail2.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey2).times(1);
		jobDetail3.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey3).times(1);
		jobDetail4.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey4).times(1);

		EasyMock.replay(logger);
		EasyMock.replay(listenerManager);
		EasyMock.replay(context1);
		EasyMock.replay(context2);
		EasyMock.replay(context3);
		EasyMock.replay(context4);
		EasyMock.replay(jobDetail1);
		EasyMock.replay(jobDetail2);
		EasyMock.replay(jobDetail3);
		EasyMock.replay(jobDetail4);

		// Execute the method
		workflowDependency.jobWasExecuted(context1, null);
		workflowDependency.jobWasExecuted(context2, null);
		workflowDependency.jobWasExecuted(context3, null);
		workflowDependency.jobWasExecuted(context4, null);

		// Verify the results EasyMock.verify(scheduler);
		EasyMock.verify(logger);
		EasyMock.verify(context1);
		EasyMock.verify(context2);
		EasyMock.verify(context3);
		EasyMock.verify(context4);
		EasyMock.verify(jobDetail1);
		EasyMock.verify(jobDetail2);
		EasyMock.verify(jobDetail3);
		EasyMock.verify(jobDetail4);
	}

	/**
	 * Test method for
	 * {@link com.spazzmania.quartz.workflow.WorkflowTask #addDependency(org.quartz.JobKey)}
	 */

	@Test
	public void testDefaultJobGeneratedError() { // Setup the Mocks
		workflowDependency.addDependency(depJobKey1);
		workflowDependency.addDependency(depJobKey2);
		workflowDependency.addDependency(depJobKey3);
		workflowDependency.addDependency(depJobKey4);

		try {
			scheduler.triggerJob(workflowDependency.getKey());
			EasyMock.expectLastCall().times(1);
			EasyMock.expect(scheduler.getListenerManager()).andStubReturn(
					listenerManager);
		} catch (Exception e) {
		}

		listenerManager.removeJobListener(workflowDependency.getName());
		EasyMock.expectLastCall().andReturn(Boolean.TRUE).times(1);

		logger.info((String) EasyMock.anyObject());
		EasyMock.expectLastCall().times(4);

		context1.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail1).anyTimes();
		context2.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail2).anyTimes();
		context3.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail3).anyTimes();
		context4.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail4).anyTimes();

		jobDetail1.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey1).times(1);
		jobDetail2.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey2).times(1);
		jobDetail3.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey3).times(1);
		jobDetail4.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey4).times(1);

		EasyMock.replay(logger);
		EasyMock.replay(listenerManager);
		EasyMock.replay(context1);
		EasyMock.replay(context2);
		EasyMock.replay(context3);
		EasyMock.replay(context4);
		EasyMock.replay(jobDetail1);
		EasyMock.replay(jobDetail2);
		EasyMock.replay(jobDetail3);
		EasyMock.replay(jobDetail4);

		JobExecutionException e = new JobExecutionException("Test Exception");

		workflowDependency.addDependency(depJobKey2,
				WorkflowStatus.GENERATED_EXCEPTION, WorkflowAction.TRIGGER_JOB);

		// Execute the method
		workflowDependency.jobWasExecuted(context1, null);
		workflowDependency.jobWasExecuted(context2, e);
		workflowDependency.jobWasExecuted(context3, null);
		workflowDependency.jobWasExecuted(context4, null);

		// Verify the results EasyMock.verify(scheduler);
		EasyMock.verify(logger);
		EasyMock.verify(context1);
		EasyMock.verify(context2);
		EasyMock.verify(context3);
		EasyMock.verify(context4);
		EasyMock.verify(jobDetail1);
		EasyMock.verify(jobDetail2);
		EasyMock.verify(jobDetail3);
		EasyMock.verify(jobDetail4);
	}

	/**
	 * Test method for
	 * {@link com.spazzmania.quartz.workflow.WorkflowTask #addDependency(org.quartz.JobKey)}
	 */

	@Test
	public void testTriggerJobWhenJobGeneratedError() { // Setup the
		// Mocks
		workflowDependency.addDependency(depJobKey1);
		workflowDependency.addDependency(depJobKey2);
		workflowDependency.addDependency(depJobKey3);
		workflowDependency.addDependency(depJobKey4);

		try {
			EasyMock.expect(scheduler.getListenerManager()).andStubReturn(
					listenerManager);
		} catch (Exception e) {
		}

		listenerManager.removeJobListener(workflowDependency.getName());
		EasyMock.expectLastCall().andReturn(Boolean.TRUE).times(1);

		listenerManager.addTriggerListener(EasyMock.anyObject(WorkflowJobVetoer.class));
		EasyMock.expectLastCall().times(1);

		logger.info((String) EasyMock.anyObject());
		EasyMock.expectLastCall().times(4);

		context1.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail1).anyTimes();
		context2.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail2).anyTimes();
		context3.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail3).anyTimes();
		context4.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail4).anyTimes();

		jobDetail1.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey1).times(1);
		jobDetail2.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey2).times(1);
		jobDetail3.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey3).times(1);
		jobDetail4.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey4).times(1);

		EasyMock.replay(logger);
		EasyMock.replay(listenerManager);
		EasyMock.replay(context1);
		EasyMock.replay(context2);
		EasyMock.replay(context3);
		EasyMock.replay(context4);
		EasyMock.replay(jobDetail1);
		EasyMock.replay(jobDetail2);
		EasyMock.replay(jobDetail3);
		EasyMock.replay(jobDetail4);

		JobExecutionException e = new JobExecutionException("Test Exception");

		workflowDependency.jobWasExecuted(context1, null);
		workflowDependency.jobWasExecuted(context2, null);
		workflowDependency.jobWasExecuted(context3, null);
		workflowDependency.jobWasExecuted(context4, e);

		// Verify the results EasyMock.verify(scheduler);
		EasyMock.verify(logger);
		EasyMock.verify(context1);
		EasyMock.verify(context2);
		EasyMock.verify(context3);
		EasyMock.verify(context4);
		EasyMock.verify(jobDetail1);
		EasyMock.verify(jobDetail2);
		EasyMock.verify(jobDetail3);
		EasyMock.verify(jobDetail4);
	}

	/**
	 * Test method for
	 * {@link com.spazzmania.quartz.workflow.WorkflowTask #addDependency(org.quartz.JobKey)}
	 */

	@Test
	public void testTriggerJobWhenJobGeneratedError2() {
		// Setup the Mocks
		workflowDependency.addDependency(depJobKey1);
		workflowDependency.addDependency(depJobKey2);
		workflowDependency.addDependency(depJobKey3);
		workflowDependency.addDependency(depJobKey4);

		try {
			scheduler.triggerJob(workflowDependency.getKey());
			EasyMock.expectLastCall().times(1);
			EasyMock.expect(scheduler.getListenerManager()).andStubReturn(
					listenerManager);
		} catch (Exception e) {
		}

		listenerManager.removeJobListener(workflowDependency.getName());
		EasyMock.expectLastCall().andReturn(Boolean.TRUE).times(1);

		logger.info((String) EasyMock.anyObject());
		EasyMock.expectLastCall().times(4);

		context1.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail1).anyTimes();
		context2.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail2).anyTimes();
		context3.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail3).anyTimes();
		context4.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail4).anyTimes();

		jobDetail1.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey1).times(1);
		jobDetail2.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey2).times(1);
		jobDetail3.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey3).times(1);
		jobDetail4.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey4).times(1);

		EasyMock.replay(listenerManager);
		EasyMock.replay(logger);
		EasyMock.replay(context1);
		EasyMock.replay(context2);
		EasyMock.replay(context3);
		EasyMock.replay(context4);
		EasyMock.replay(jobDetail1);
		EasyMock.replay(jobDetail2);
		EasyMock.replay(jobDetail3);
		EasyMock.replay(jobDetail4);

		JobExecutionException e = new JobExecutionException("Test Exception");

		workflowDependency.addDependency(depJobKey2,
				WorkflowStatus.GENERATED_EXCEPTION, WorkflowAction.TRIGGER_JOB);

		// Execute the method
		workflowDependency.jobWasExecuted(context1, null);
		workflowDependency.jobWasExecuted(context2, e);
		workflowDependency.jobWasExecuted(context3, null);
		workflowDependency.jobWasExecuted(context4, null);

		// Verify the results EasyMock.verify(scheduler);
		EasyMock.verify(logger);
		EasyMock.verify(context1);
		EasyMock.verify(context2);
		EasyMock.verify(context3);
		EasyMock.verify(context4);
		EasyMock.verify(jobDetail1);
		EasyMock.verify(jobDetail2);
		EasyMock.verify(jobDetail3);
		EasyMock.verify(jobDetail4);
	}

	/**
	 * Test method for
	 * {@link com.spazzmania.quartz.workflow.WorkflowTask #addDependency(org.quartz.JobKey)}
	 */

	@Test
	public void testTriggerJobWhenJobExecutionVetoed() {
		// Setup the Mocks
		workflowDependency.addDependency(depJobKey1);
		workflowDependency.addDependency(depJobKey2);
		workflowDependency.addDependency(depJobKey3);
		workflowDependency.addDependency(depJobKey4);

		listenerManager.removeJobListener(workflowDependency.getName());
		EasyMock.expectLastCall().andReturn(Boolean.TRUE).times(1);

		logger.info((String) EasyMock.anyObject());
		EasyMock.expectLastCall().times(4);

		context1.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail1).anyTimes();
		context2.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail2).anyTimes();
		context3.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail3).anyTimes();
		context4.getJobDetail();
		EasyMock.expectLastCall().andReturn(jobDetail4).anyTimes();

		jobDetail1.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey1).times(1);
		jobDetail2.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey2).times(1);
		jobDetail3.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey3).times(1);
		jobDetail4.getKey();
		EasyMock.expectLastCall().andReturn(depJobKey4).times(1);

		EasyMock.replay(logger);
		EasyMock.replay(listenerManager);
		EasyMock.replay(context1);
		EasyMock.replay(context2);
		EasyMock.replay(context3);
		EasyMock.replay(context4);
		EasyMock.replay(jobDetail1);
		EasyMock.replay(jobDetail2);
		EasyMock.replay(jobDetail3);
		EasyMock.replay(jobDetail4);

		workflowDependency.addDependency(depJobKey2,
				WorkflowStatus.GENERATED_EXCEPTION, WorkflowAction.TRIGGER_JOB);
		workflowDependency.addDependency(depJobKey2, WorkflowStatus.VETOED,
				WorkflowAction.TRIGGER_JOB);

		// Execute the method
		workflowDependency.jobWasExecuted(context1, null);
		workflowDependency.jobExecutionVetoed(context2);
		workflowDependency.jobWasExecuted(context3, null);
		workflowDependency.jobWasExecuted(context4, null);

		// Verify the results EasyMock.verify(scheduler);
		EasyMock.verify(logger);
		EasyMock.verify(context1);
		EasyMock.verify(context2);
		EasyMock.verify(context3);
		EasyMock.verify(context4);
		EasyMock.verify(jobDetail1);
		EasyMock.verify(jobDetail2);
		EasyMock.verify(jobDetail3);
		EasyMock.verify(jobDetail4);
	}

	/**
	 * Test method for
	 * {@link com.spazzmania.quartz.workflow.WorkflowTask #addDependency(org.quartz.JobKey)}
	 */
	  
	  @Test public void testTriggerJobWhenOtherJobStarts() { // Setup the Mocks
	  
	        workflowDependency = new WorkflowTask(jobDetail);
	        workflowDependency.setLogger(logger);
	        workflowDependency.setScheduler(scheduler);
	  
	        listenerManager.removeJobListener(workflowDependency.getName());
	        EasyMock.expectLastCall().andReturn(Boolean.TRUE).times(1);
	  
	        logger.info((String) EasyMock.anyObject());
	        EasyMock.expectLastCall().times(1);
	  
	        context1.getJobDetail();
	        EasyMock.expectLastCall().andReturn(jobDetail1).anyTimes();
	  
	        jobDetail1.getKey();
	        EasyMock.expectLastCall().andReturn(depJobKey1).times(1);
	  
	        jobDetail.getKey();
	        EasyMock.expectLastCall().andReturn(jobKey).anyTimes();
	        
	        EasyMock.replay(logger);
	        EasyMock.replay(listenerManager); EasyMock.replay(context1);
	        EasyMock.replay(jobDetail1);
	  
	        workflowDependency.addDependency(depJobKey1,
	        WorkflowStatus.IN_PROGRESS, WorkflowAction.TRIGGER_JOB);
	  
	        // Execute the method 
	        workflowDependency.jobToBeExecuted(context1);
	  
	        // Verify the results 
	        EasyMock.verify(logger); EasyMock.verify(context1);
	        EasyMock.verify(jobDetail1); }
}