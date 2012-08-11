package com.spazzmania.quartz.workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;

public class WorkflowParser {

	private Integer groupId = 0;
	private Integer jobId = 0;
	private String groupName;
	private Map<JobKey,Boolean> defaultDependencies;

	public static String WORKFLOW_JOBS = "workflow-jobs";
	public static String JOB_CLASS = "job-class";
	public static String JOB_KEY = "job-key";
	public static String JOB_DATA_MAP = "job-data-map";
	public static String DEPENDENCIES = "dependencies";

	/**
	 * @return the groupName
	 */
	public String getGroupName() {
		return groupName;
	}

	/**
	 * @param groupName
	 *            the groupName to set
	 */
	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}

	public List<WorkflowTask> parse(String workflowJson) {
		groupId++;
		setGroupName("WorkflowGroup-" + groupId);
		defaultDependencies = new HashMap<JobKey,Boolean>();

		List<WorkflowTask> tasks = new ArrayList<WorkflowTask>();
		JSONParser parser = new JSONParser();
		try {
			JSONObject workflowObject = (JSONObject) parser.parse(workflowJson);
			JSONArray workflowJobs = (JSONArray) workflowObject
					.get(WORKFLOW_JOBS);
			@SuppressWarnings("unchecked")
			Iterator<JSONObject> iterator = (Iterator<JSONObject>) workflowJobs
					.iterator();
			while (iterator.hasNext()) {
				tasks.add(parseTask(iterator.next()));
			}
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}

		checkDefaultDependencies(tasks);

		return tasks;
	}

	public WorkflowTask parseTask(JSONObject taskObj) {
		JobDetail jobDetail = parseJobDetail(taskObj);
		List<WorkflowDependency> dependencies = parseJobDependencies(jobDetail.getKey(),taskObj);
		WorkflowTask task = new WorkflowTask(jobDetail);
		task.setDependencies(dependencies);
		return task;
	}

	public JobDetail parseJobDetail(JSONObject taskObj) {
		Class<Job> jobClazz = parseJobClass(taskObj);
		JobKey jobKey = parseJobKey(taskObj);
		JobDataMap jobDataMap = parseJobDataMap(taskObj);

		return org.quartz.JobBuilder.newJob(jobClazz).withIdentity(jobKey)
				.usingJobData(jobDataMap).storeDurably().build();
	}

	@SuppressWarnings("unchecked")
	public Class<Job> parseJobClass(JSONObject taskObj) {
		Class<?> jobClazz;
		if (!taskObj.containsKey("job-class")) {
			throw new ExceptionInInitializerError(
					"Invalid workflow-job configuration - missing 'job-class'");
		}
		String clazzName = (String) taskObj.get(JOB_CLASS);
		try {
			jobClazz = Class.forName(clazzName);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}

		if (!Job.class.isAssignableFrom(jobClazz)) {
			throw new RuntimeException("Invalid Job class: " + clazzName);
		}
		return (Class<Job>) jobClazz;
	}

	public JobKey parseJobKey(JSONObject taskObj) {
		jobId++;
		String jobName = "WorkflowJob-" + jobId;
		String jobGroupName = getGroupName();

		JobKey jobKey = new JobKey(jobName, jobGroupName);

		if (taskObj.containsKey(JOB_KEY)) {
			String jobKeyStr = (String) taskObj.get(JOB_KEY);
			jobKey = parseJobKeyStr(jobKeyStr, getGroupName());
		}

		return jobKey;
	}

	public JobKey parseJobKeyStr(String jobKeyStr, String defaultGroup) {
		String jobName = jobKeyStr;
		String jobGroupName = defaultGroup;

		if (Pattern.matches(".+[\\.\\/]{1}.+", jobKeyStr)) {
			Pattern p = Pattern.compile("[\\.\\/]{1}");
			String[] jobKeyParts = p.split(jobKeyStr);
			jobName = jobKeyParts[0];
			jobGroupName = jobKeyParts[1];
		}

		return new JobKey(jobName, jobGroupName);
	}

	public JobDataMap parseJobDataMap(JSONObject taskObj) {
		JobDataMap dataMap = new JobDataMap();
		if (!taskObj.containsKey(JOB_DATA_MAP)) {
			return dataMap;
		}

		JSONObject dataMapObj = (JSONObject) taskObj.get(JOB_DATA_MAP);
		for (Object k : dataMapObj.keySet()) {
			dataMap.put((String) k, (String) dataMapObj.get(k));
		}
		return dataMap;
	}

	public List<WorkflowDependency> parseJobDependencies(JobKey jobKey, JSONObject taskObj) {
		List<WorkflowDependency> dependencies = new ArrayList<WorkflowDependency>();
		if (!taskObj.containsKey(DEPENDENCIES)) {
			defaultDependencies.put(jobKey, Boolean.TRUE);
			return dependencies;
		}

		JSONArray deps = (JSONArray) taskObj.get(DEPENDENCIES);
		if (deps != null) {
			@SuppressWarnings("unchecked")
			Iterator<JSONObject> depi = (Iterator<JSONObject>) deps.iterator();
			while (depi.hasNext()) {
				Object depObj = depi.next();
				if (JSONObject.class.isInstance(depObj)) {
					dependencies
							.add(parseComplexDependency((JSONObject) depObj));
				} else {
					dependencies.add(new WorkflowDependency(parseJobKeyStr(
							(String) depObj, getGroupName())));
				}
			}
		}

		return dependencies;
	}

	public WorkflowDependency parseComplexDependency(JSONObject dependencyObj) {
		WorkflowDependency dependency = null;
		for (Object jobKeyStr : dependencyObj.keySet()) {
			dependency = new WorkflowDependency(parseJobKeyStr(
					(String) jobKeyStr, getGroupName()));
			JSONObject statusActions = (JSONObject) dependencyObj
					.get(jobKeyStr);
			for (Object status : statusActions.keySet()) {
				Object action = statusActions.get(status);
				dependency.setStatusAction(
						WorkflowStatus.valueOf((String) status),
						WorkflowAction.valueOf((String) action));
			}
			break;
		}
		if (dependency == null) {
			throw new RuntimeException("Invalid dependency declaration: "
					+ dependencyObj);
		}
		return dependency;
	}

	public void checkDefaultDependencies(List<WorkflowTask> tasks) {
		JobKey previousJob = null;
		for (WorkflowTask task : tasks) {
			if (previousJob != null) {
				if (defaultDependencies.containsKey(task.getKey())) {
					task.getDependencies().put(previousJob,
							new WorkflowDependency(previousJob));
				}
			}
			previousJob = task.getKey();
		}
	}
}
