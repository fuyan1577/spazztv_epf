package com.spazzmania.quartz.workflow;

import java.util.List;

public class WorkflowParserTest3 {

	public static String workflowJobs1 = "{"
			+ "\"workflow-jobs\": ["
			+ "	{\"job-class\":\"com.spazzmania.misc.TestJob\"},"
			+ "	{\"job-class\":\"com.spazzmania.misc.TestJob\"},"
			+ "	{\"job-class\":\"com.spazzmania.misc.TestJob\",\"dependencies\":[]},"
			+ "	{\"job-class\":\"com.spazzmania.misc.TestJob\"}," 
			+ "]" 
			+ "}";

	public static void main(String[] args) {
		WorkflowParser parser = new WorkflowParser();
		List<WorkflowTask> tasks = parser.parse(workflowJobs1);

		for (WorkflowTask task : tasks) {
			System.out.println("Task: " + task.getKey());
		}
	}
}
