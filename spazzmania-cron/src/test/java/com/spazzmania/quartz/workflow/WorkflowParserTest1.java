/**
 * 
 */
package com.spazzmania.quartz.workflow;

import java.util.Iterator;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.spazzmania.quartz.workflow.WorkflowParser;
import com.spazzmania.quartz.workflow.WorkflowTask;

/**
 * @author billintj
 * 
 */
public class WorkflowParserTest1 {
	public static String workflowJobs1 = "{"
			+ "\"workflow-jobs\": ["
			+ "	{"
			+ "		\"job-class\":\"com.spazzmania.misc.TestJob\","
			+ "		\"job-key\":\"job1/group1\","
			+ "		\"job-data-map\":{\"params\":\"asdflj asdflkj asdflkj asdflkj\"}, "
			+ "		\"dependencies\":[]"
			+ "	},"
			+ "	{"
			+ "		\"job-class\":\"com.spazzmania.misc.TestJob\","
			+ "		\"job-key\":\"job2/group1\","
			+ "		\"job-data-map\":{\"params\":\"asdflj asdflkj asdflkj asdflkj\"},"
			+ "		\"dependencies\": [\"job1/group1\"]"
			+ "	},"
			+ "	{"
			+ "		\"job-class\":\"com.spazzmania.misc.TestJob\","
			+ "		\"job-key\":\"job3/group1\","
			+ "		\"job-data-map\":{\"params\":\"asdflj asdflkj asdflkj asdflkj\"}, "
			+ "		\"dependencies\":["
			+ "			{\"job1/group1\":{\"GENERATED_EXCEPTION\":\"TRIGGER_JOB\",\"VETOED\":\"TRIGGER_JOB\"}},"
			+ "			\"job2/group1\","
			+ "			\"job3/group1\","
			+ "			\"job4/group1\""
			+ "		]"
			+ "	},"
			+ "	{"
			+ "		\"job-class\":\"com.spazzmania.misc.TestJob\","
			+ "		\"job-key\":\"job4/group1\","
			+ "		\"job-data-map\":{\"params\":\"asdflj asdflkj asdflkj asdflkj\"}, "
			+ "		\"dependencies\":["
			+ "			{\"job1/group1\":{\"GENERATED_EXCEPTION\":\"TRIGGER_JOB\",\"VETOED\":\"TRIGGER_JOB\"}},"
			+ "			\"job2/group1\"," + "			\"job3/group1\","
			+ "			\"job3/group1\"" + "		]" + "	}" + "]" + "}";

	public static void main(String[] args) {
		WorkflowParser parser = new WorkflowParser();
		List<WorkflowTask> tasks = parser.parse(workflowJobs1);

		for (WorkflowTask task : tasks) {
			System.out.println("Task: " + task.getKey());
		}
	}
}
