/**
 * 
 */
package com.spazzmania.quartz.workflow;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.quartz.Calendar;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.ListenerManager;
import org.quartz.Scheduler;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.quartz.SchedulerMetaData;
import org.quartz.Trigger;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.UnableToInterruptJobException;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.JobFactory;

/**
 * @author billintj
 *
 */
public class MockScheduler implements Scheduler {
	
	private ListenerManager listenerManager;
	
	public MockScheduler(ListenerManager listenerManager) {
		this.listenerManager = listenerManager;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#addCalendar(java.lang.String, org.quartz.Calendar, boolean, boolean)
	 */
	@Override
	public void addCalendar(String arg0, Calendar arg1, boolean arg2,
			boolean arg3) throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#addJob(org.quartz.JobDetail, boolean)
	 */
	@Override
	public void addJob(JobDetail arg0, boolean arg1) throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#checkExists(org.quartz.JobKey)
	 */
	@Override
	public boolean checkExists(JobKey arg0) throws SchedulerException {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#checkExists(org.quartz.TriggerKey)
	 */
	@Override
	public boolean checkExists(TriggerKey arg0) throws SchedulerException {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#clear()
	 */
	@Override
	public void clear() throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#deleteCalendar(java.lang.String)
	 */
	@Override
	public boolean deleteCalendar(String arg0) throws SchedulerException {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#deleteJob(org.quartz.JobKey)
	 */
	@Override
	public boolean deleteJob(JobKey arg0) throws SchedulerException {
		// TODO Auto-generated method stub
		return true;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#deleteJobs(java.util.List)
	 */
	@Override
	public boolean deleteJobs(List<JobKey> arg0) throws SchedulerException {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getCalendar(java.lang.String)
	 */
	@Override
	public Calendar getCalendar(String arg0) throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getCalendarNames()
	 */
	@Override
	public List<String> getCalendarNames() throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getContext()
	 */
	@Override
	public SchedulerContext getContext() throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getCurrentlyExecutingJobs()
	 */
	@Override
	public List<JobExecutionContext> getCurrentlyExecutingJobs()
			throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getJobDetail(org.quartz.JobKey)
	 */
	@Override
	public JobDetail getJobDetail(JobKey arg0) throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getJobGroupNames()
	 */
	@Override
	public List<String> getJobGroupNames() throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getJobKeys(org.quartz.impl.matchers.GroupMatcher)
	 */
	@Override
	public Set<JobKey> getJobKeys(GroupMatcher<JobKey> arg0)
			throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getListenerManager()
	 */
	@Override
	public ListenerManager getListenerManager() throws SchedulerException {
		// TODO Auto-generated method stub
		return listenerManager;
	}
	
	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getMetaData()
	 */
	@Override
	public SchedulerMetaData getMetaData() throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getPausedTriggerGroups()
	 */
	@Override
	public Set<String> getPausedTriggerGroups() throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getSchedulerInstanceId()
	 */
	@Override
	public String getSchedulerInstanceId() throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getSchedulerName()
	 */
	@Override
	public String getSchedulerName() throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getTrigger(org.quartz.TriggerKey)
	 */
	@Override
	public Trigger getTrigger(TriggerKey arg0) throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getTriggerGroupNames()
	 */
	@Override
	public List<String> getTriggerGroupNames() throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getTriggerKeys(org.quartz.impl.matchers.GroupMatcher)
	 */
	@Override
	public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> arg0)
			throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getTriggerState(org.quartz.TriggerKey)
	 */
	@Override
	public TriggerState getTriggerState(TriggerKey arg0)
			throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#getTriggersOfJob(org.quartz.JobKey)
	 */
	@Override
	public List<? extends Trigger> getTriggersOfJob(JobKey arg0)
			throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#interrupt(org.quartz.JobKey)
	 */
	@Override
	public boolean interrupt(JobKey arg0) throws UnableToInterruptJobException {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#interrupt(java.lang.String)
	 */
	@Override
	public boolean interrupt(String arg0) throws UnableToInterruptJobException {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#isInStandbyMode()
	 */
	@Override
	public boolean isInStandbyMode() throws SchedulerException {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#isShutdown()
	 */
	@Override
	public boolean isShutdown() throws SchedulerException {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#isStarted()
	 */
	@Override
	public boolean isStarted() throws SchedulerException {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#pauseAll()
	 */
	@Override
	public void pauseAll() throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#pauseJob(org.quartz.JobKey)
	 */
	@Override
	public void pauseJob(JobKey arg0) throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#pauseJobs(org.quartz.impl.matchers.GroupMatcher)
	 */
	@Override
	public void pauseJobs(GroupMatcher<JobKey> arg0) throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#pauseTrigger(org.quartz.TriggerKey)
	 */
	@Override
	public void pauseTrigger(TriggerKey arg0) throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#pauseTriggers(org.quartz.impl.matchers.GroupMatcher)
	 */
	@Override
	public void pauseTriggers(GroupMatcher<TriggerKey> arg0)
			throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#rescheduleJob(org.quartz.TriggerKey, org.quartz.Trigger)
	 */
	@Override
	public Date rescheduleJob(TriggerKey arg0, Trigger arg1)
			throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#resumeAll()
	 */
	@Override
	public void resumeAll() throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#resumeJob(org.quartz.JobKey)
	 */
	@Override
	public void resumeJob(JobKey arg0) throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#resumeJobs(org.quartz.impl.matchers.GroupMatcher)
	 */
	@Override
	public void resumeJobs(GroupMatcher<JobKey> arg0) throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#resumeTrigger(org.quartz.TriggerKey)
	 */
	@Override
	public void resumeTrigger(TriggerKey arg0) throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#resumeTriggers(org.quartz.impl.matchers.GroupMatcher)
	 */
	@Override
	public void resumeTriggers(GroupMatcher<TriggerKey> arg0)
			throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#scheduleJob(org.quartz.Trigger)
	 */
	@Override
	public Date scheduleJob(Trigger arg0) throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#scheduleJob(org.quartz.JobDetail, org.quartz.Trigger)
	 */
	@Override
	public Date scheduleJob(JobDetail arg0, Trigger arg1)
			throws SchedulerException {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#scheduleJobs(java.util.Map, boolean)
	 */
	@Override
	public void scheduleJobs(Map<JobDetail, List<Trigger>> arg0, boolean arg1)
			throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#setJobFactory(org.quartz.spi.JobFactory)
	 */
	@Override
	public void setJobFactory(JobFactory arg0) throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#shutdown()
	 */
	@Override
	public void shutdown() throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#shutdown(boolean)
	 */
	@Override
	public void shutdown(boolean arg0) throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#standby()
	 */
	@Override
	public void standby() throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#start()
	 */
	@Override
	public void start() throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#startDelayed(int)
	 */
	@Override
	public void startDelayed(int arg0) throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#triggerJob(org.quartz.JobKey)
	 */
	@Override
	public void triggerJob(JobKey arg0) throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#triggerJob(org.quartz.JobKey, org.quartz.JobDataMap)
	 */
	@Override
	public void triggerJob(JobKey arg0, JobDataMap arg1)
			throws SchedulerException {
		// TODO Auto-generated method stub

	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#unscheduleJob(org.quartz.TriggerKey)
	 */
	@Override
	public boolean unscheduleJob(TriggerKey arg0) throws SchedulerException {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see org.quartz.Scheduler#unscheduleJobs(java.util.List)
	 */
	@Override
	public boolean unscheduleJobs(List<TriggerKey> arg0)
			throws SchedulerException {
		// TODO Auto-generated method stub
		return false;
	}

}
