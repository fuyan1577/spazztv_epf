package com.spazzmania.quartz.workflow;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;
import org.quartz.JobKey;

public class WorkflowStatusActionTest {
	
	WorkflowDependency jobDependency;
	
	@Before
	public void setUp() throws Exception {
		jobDependency = new WorkflowDependency();
	}

	@Test
	public void testDefaultActionPendingWait() {
		jobDependency.setStatus(WorkflowStatus.PENDING);
		Assert.assertTrue("Expected WAIT action for PENDING status",jobDependency.getAction() == WorkflowAction.WAIT);
	}

	@Test
	public void testDefaultActionInProgressWait() {
		jobDependency.setStatus(WorkflowStatus.IN_PROGRESS);
		Assert.assertTrue("Expected WAIT action for IN_PROGRESS status",jobDependency.getAction() == WorkflowAction.WAIT);
	}

	@Test
	public void testDefaultActionVetoedExit() {
		jobDependency.setStatus(WorkflowStatus.VETOED);
		Assert.assertTrue("Expected EXIT action for VETOED status",jobDependency.getAction() == WorkflowAction.EXIT);
	}

	@Test
	public void testDefaultActionGeneratedExceptionExit() {
		jobDependency.setStatus(WorkflowStatus.GENERATED_EXCEPTION);
		Assert.assertTrue("Expected EXIT action for GENERATED_EXCEPTION status",jobDependency.getAction() == WorkflowAction.EXIT);
	}

	@Test
	public void testDefaultActionGeneratedCompletedTriggerJob() {
		jobDependency.setStatus(WorkflowStatus.COMPLETED);
		Assert.assertTrue("Expected WAIT action for COMPLETED status",jobDependency.getAction() == WorkflowAction.TRIGGER_JOB);
	}

	@Test
	public void testGetActionPendingWait() {
		jobDependency.setStatusAction(WorkflowStatus.PENDING,WorkflowAction.WAIT);
		jobDependency.setStatus(WorkflowStatus.PENDING);
		Assert.assertTrue("Expected WAIT action for PENDING status",jobDependency.getAction() == WorkflowAction.WAIT);
	}

	@Test
	public void testGetActionPendingExit() {
		jobDependency.setStatusAction(WorkflowStatus.PENDING,WorkflowAction.EXIT);
		jobDependency.setStatus(WorkflowStatus.PENDING);
		Assert.assertTrue("Expected EXIT action for PENDING status",jobDependency.getAction() == WorkflowAction.EXIT);
	}

	@Test
	public void testGetActionInProgressWait() {
		jobDependency.setStatusAction(WorkflowStatus.IN_PROGRESS,WorkflowAction.WAIT);
		jobDependency.setStatus(WorkflowStatus.IN_PROGRESS);
		Assert.assertTrue("Expected WAIT action for IN_PROGRESS status",jobDependency.getAction() == WorkflowAction.WAIT);
	}

	@Test
	public void testGetActionInProgressExit() {
		jobDependency.setStatusAction(WorkflowStatus.IN_PROGRESS,WorkflowAction.EXIT);
		jobDependency.setStatus(WorkflowStatus.IN_PROGRESS);
		Assert.assertTrue("Expected EXIT action for IN_PROGRESS status",jobDependency.getAction() == WorkflowAction.EXIT);
	}

	@Test
	public void testGetActionVetoExit() {
		jobDependency.setStatusAction(WorkflowStatus.VETOED,WorkflowAction.EXIT);
		jobDependency.setStatus(WorkflowStatus.VETOED);
		Assert.assertTrue("Expected EXIT action for VETOED status",jobDependency.getAction() == WorkflowAction.EXIT);
	}

	@Test
	public void testGetActionVetoTriggerJob() {
		jobDependency.setStatusAction(WorkflowStatus.VETOED,WorkflowAction.TRIGGER_JOB);
		jobDependency.setStatus(WorkflowStatus.VETOED);
		Assert.assertTrue("Expected EXIT action for VETOED status",jobDependency.getAction() == WorkflowAction.TRIGGER_JOB);
	}

	@Test
	public void testGetActionExitExceptionExit() {
		jobDependency.setStatusAction(WorkflowStatus.GENERATED_EXCEPTION,WorkflowAction.EXIT);
		jobDependency.setStatus(WorkflowStatus.GENERATED_EXCEPTION);
		Assert.assertTrue("Expected EXIT action for GENERATED_EXCEPTION status",jobDependency.getAction() == WorkflowAction.EXIT);
	}
	
	@Test
	public void testGetActionExitExceptionTriggerJob() {
		jobDependency.setStatusAction(WorkflowStatus.GENERATED_EXCEPTION,WorkflowAction.TRIGGER_JOB);
		jobDependency.setStatus(WorkflowStatus.GENERATED_EXCEPTION);
		Assert.assertTrue("Expected TRIGGER_JOB action for GENERATED_EXCEPTION status",jobDependency.getAction() == WorkflowAction.TRIGGER_JOB);
	}
	
	@Test
	public void testGetActionCompletedExit() {
		jobDependency.setStatusAction(WorkflowStatus.COMPLETED,WorkflowAction.EXIT);
		jobDependency.setStatus(WorkflowStatus.COMPLETED);
		Assert.assertTrue("Expected EXIT action for COMPLETED status",jobDependency.getAction() == WorkflowAction.EXIT);
	}

	@Test
	public void testGetActionCompletedTriggerJob() {
		jobDependency.setStatusAction(WorkflowStatus.COMPLETED,WorkflowAction.TRIGGER_JOB);
		jobDependency.setStatus(WorkflowStatus.COMPLETED);
		Assert.assertTrue("Expected TRIGGER_JOB action for COMPLETED status",jobDependency.getAction() == WorkflowAction.TRIGGER_JOB);
	}
	
}
