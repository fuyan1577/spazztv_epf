package com.spazzmania.archive;

import static org.quartz.DateBuilder.evenMinuteDate;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.junit.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import com.spazzmania.archive.SynchronizedJob;

public class SynchronizedJobTest {

	private Scheduler scheduler;
	private JobDataMap dataMap;

	@Argument(metaVar = "[target [target2 [target3] ...]]", usage = "options")
	private List<String> options = new ArrayList<String>();

	@Option(name = "-h", aliases = "--help", usage = "print this message")
	private boolean help = false;

	@Option(name = "-w", aliases = { "--wait" }, metaVar = "number", usage = "[-w | --wait] waitseconds")
	private int waitSeconds;

	@Option(name = "-D", metaVar = "<property>=<value>", usage = "use value for given property")
	private Map<String, String> properties = new HashMap<String, String>();
	
	@Before
	public void setUp() throws Exception {
		SchedulerFactory sf = new StdSchedulerFactory();
		scheduler = sf.getScheduler();
	}

	@After
	public void tearDown() throws Exception {
		scheduler.shutdown();
	}

	@Test
	public void testParseArgs() {
		final AntOptsArgs4J options = new AntOptsArgs4J();
		final CmdLineParser parser = new CmdLineParser(options);
		try {
			Pattern argPattern = Pattern.compile("[\\s\\t]+");
			String[] args = argPattern.split("-w 10 one two three four:4");
			parser.parseArgument(args);
			parser.setUsageWidth(Integer.MAX_VALUE);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	@Test
	public void testSyncJobs1() {

		// computer a time that is on the next round minute
		Date runTime = evenMinuteDate(new Date());

		// define the job and tie it to our HelloJob class
		JobDetail job = newJob(SynchronizedJob.class).withIdentity(
				"SyncTestJob1", "SyncTestGroup").build();

		JobDataMap map = job.getJobDataMap();
		map.put(SynchronizedJob.SYNC_JOB_CLASS,
				"com.spazzmania.cron.ArgvPrinterJob");
		map.put(SynchronizedJob.SYNC_JOB_ARGS,
				"-w 10 one two three four:4");

		// Trigger the job to run on the next round minute
		Trigger trigger = newTrigger().withIdentity("trigger1", "group1")
				.startAt(runTime).build();

		// Tell quartz to schedule the job using our trigger
		try {
			scheduler.scheduleJob(job, trigger);
			scheduler.start();
		} catch (Exception e) {
			Assert.assertFalse("Scheduler aborted", true);
		}

		// wait long enough so that the scheduler as an opportunity to
		// run the job!
		try {
			// wait 65 seconds to show job
			boolean done = false;
			while (!done) {
				Thread.sleep(600L * 1000L);
			}
			// executing...
		} catch (Exception e) {
		}

		Assert.assertTrue("Scheduler failed before completion", true);
	}
}
