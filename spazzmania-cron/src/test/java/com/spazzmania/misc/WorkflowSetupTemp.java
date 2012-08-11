package com.spazzmania.misc;

import java.util.Iterator;

import org.hamcrest.core.IsInstanceOf;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public class WorkflowSetupTemp {
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
		try {
			JSONParser jsonParser = new JSONParser();
			JSONObject jsonObject = (JSONObject) jsonParser
					.parse(workflowJobs1);

			// loop array
			JSONArray msg = (JSONArray) jsonObject.get("workflow-jobs");
			@SuppressWarnings("unchecked")
			Iterator<JSONObject> iterator = (Iterator<JSONObject>) msg
					.iterator();
			while (iterator.hasNext()) {
				JSONObject job = iterator.next();
				System.out.println(job);
				JSONArray deps = (JSONArray) job.get("dependencies");
				if (deps != null) {
					@SuppressWarnings("unchecked")
					Iterator<JSONObject> depi = (Iterator<JSONObject>) deps
							.iterator();
					while (depi.hasNext()) {
						Object obj = depi.next();
						if (JSONObject.class.isInstance(obj)) {
							System.out.println(obj);
							JSONObject d = (JSONObject)obj;
							for (Object k1 : d.keySet()) {
								JSONObject d2 = (JSONObject)d.get(k1);
								System.out.println("Custom Dependency: " + k1);
								for (Object k2 : d2.keySet()) {
									Object v2 = d2.get(k2);
									System.out.println(String.format("Status %s = Action %s",k2,v2));
								}
							}
						}
						else {
							System.out.println("Default Dependency: " + obj);
						}
					}
				}
			}

		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
