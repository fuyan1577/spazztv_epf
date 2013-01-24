package com.spazztv.cron;

import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameter.ParameterType;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@ContextConfiguration(locations={"/launch-context.xml"})
@RunWith(SpringJUnit4ClassRunner.class)
public class ExampleJobConfigurationTests {

	@Autowired
	private JobLauncher jobLauncher;
	
	@Autowired
	private Job job;
	
	private JobParameters jobParameters;
	
	@Before
	public void setUp() {
		Map<String,JobParameter> jobParamMap = new HashMap<String,JobParameter>();
		JobParameter parameter = new JobParameter(new java.util.Date());
		ParameterType pType = parameter.getType();
		java.util.Date pDate = (java.util.Date)parameter.getValue();
		if (pType.equals(ParameterType.DATE)) {
			System.out.printf("Parameter Type is DATE: %s\n", pDate);
		}
		jobParamMap.put("RunDate", parameter);
		jobParameters = new JobParameters(jobParamMap);
	}
	
	@Test
	public void testSimpleProperties() throws Exception {
		assertNotNull(jobLauncher);
	}
	
	@Test
	public void testLaunchJob() throws Exception {
//		jobLauncher.run(job, new JobParameters());
		jobLauncher.run(job, jobParameters);
	}
	
}
