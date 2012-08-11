package com.spazzmania.misc;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobListener;
import org.quartz.ListenerManager;
import org.quartz.Scheduler;

import com.spazzmania.quartz.workflow.WorkflowTask;

public class EasyMockQuartzTest {

	private Scheduler scheduler;
	private ListenerManager listenerManager;
	private JobKey jobKey = new JobKey("job1","group1");
	private WorkflowTask jobListener;
	private JobDetail jobDetail;

	@Before
	public void setUp() throws Exception {
		listenerManager = EasyMock.createMock(ListenerManager.class);
		scheduler = EasyMock.createMock(Scheduler.class);
		jobDetail = EasyMock.createMock(JobDetail.class);
	}

	@Test
	public void test() {
		jobDetail.getKey();
		EasyMock.expectLastCall().andReturn(jobKey).anyTimes();
		EasyMock.replay(jobDetail);
		
		jobListener = new WorkflowTask(jobDetail);
		
		listenerManager.getJobListener(jobListener.getName());
		EasyMock.expectLastCall().andReturn(jobListener).times(1);
		try {
			scheduler.getListenerManager();
			EasyMock.expectLastCall().andReturn(listenerManager).anyTimes();
		} catch (Exception e) {
		}
		EasyMock.replay(listenerManager);
		EasyMock.replay(scheduler);
		try {
			ListenerManager lm = scheduler.getListenerManager();
			JobListener jl = lm.getJobListener(jobListener.getName());
		} catch (Exception e) {
		}
		EasyMock.verify(scheduler);
		EasyMock.verify(listenerManager);
	}
}
