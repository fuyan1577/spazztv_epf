/**
 * 
 */
package com.spazzmania.cron;

import org.kohsuke.args4j.Option;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spazzmania.epf.EPFConnector;
import com.spazzmania.epf.EPFConnectorImpl;
import com.spazzmania.epf.EPFDataDownloadUtil;
import com.spazzmania.epf.EPFReleaseUtil;
import com.spazzmania.model.util.SpazzDBUtil;

/**
 * EPFDataDownloadJob - a job which downloads the latest EPF Data Release files
 * which occur after the last download dates.
 * 
 * <p>
 * This is a Quartz Scheduler Job which builds and links the EPFDataDownloadUtil
 * class and then executes it's run() method.
 * <p>
 * This job may be run from Quarts Scheduler by setting it's parameters in a
 * JobDataMap. This job may also be run from the command line by setting the
 * parameters as command line arguments.
 * 
 * <p>
 * The following arguments are required to run this job:
 * <table border=1>
 * <tr>
 * <th>JobDataMap Key</th>
 * <th>Command Arg</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>database_host</td>
 * <td>--host=DBURL</td>
 * <td>Spazz DB Url</td>
 * </tr>
 * <tr>
 * <td>database_user</td>
 * <td>--username=DBUSER</td>
 * <td>Spazz DB User</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>database_pass=DBPASS</td>
 * <td>--password</td>
 * <td>Spazz DB Pass</td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>database</td>
 * <td>--database=DBNAME</td>
 * <td>Spazz DB Schema</td>
 * </tr>
 * <tr>
 * <td>epf_username</td>
 * <td>--epf_username=EPFUSER</td>
 * <td>EPF User</td>
 * </tr>
 * <tr>
 * <td>epf_password</td>
 * <td>--epf_password=EPFPASS</td>
 * <td>EPF Pass</td>
 * </tr>
 * <tr>
 * <td>download_directory</td>
 * <td>--download_dir=PATH</td>
 * <td>EPF Download Dir</td>
 * </tr>
 * <tr>
 * <td>timeout</td>
 * <td>--timeout=MINUTES</td>
 * <td>EPF Job Timeout Minutes</td>
 * </tr>
 * <tr>
 * <td>logger_name</td>
 * <td>n/a</td>
 * <td>Quartz Job Logger Name</td>
 * </tr>
 * <tr>
 * <td>sleep_interval</td>
 * <td>--sleep_interval=SECONDS</td>
 * <td>Interval Between EPF Retries</td>
 * </tr>
 * </table>
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFDataDownloadJob implements Job {

	public static String DATABASE_HOST = "database_host";
	public static String DATABASE_USER = "database_user";
	public static String DATABASE_PASS = "database_pass";
	public static String DATABASE = "database";
	public static String EPF_USERNAME = "epf_username";
	public static String EPF_PASSWORD = "epf_password";
	public static String DOWNLOAD_DIRECTORY = "download_directory";
	public static String TIMEOUT = "timeout";
	public static String LOGGER_NAME = "logger_name";
	public static String SLEEP_INTERVAL = "sleep_interval";

	@Option(name = "--username", metaVar = "<username>")
	private String username;

	@Option(name = "--password", metaVar = "<password>")
	private String password;

	@Option(name = "--database", metaVar = "<database>")
	private String database;

	@Option(name = "--host", metaVar = "<host>")
	private String host;

	@Option(name = "--epf_username", metaVar = "<database>")
	private String epfUsername;

	@Option(name = "--epf_password", metaVar = "<database>")
	private String epfPassword;

	@Option(name = "--epf_baseurl", metaVar = "<database>")
	private String epfBaseUrl = EPFConnector.EPF_BASE_URL;

	@Option(name = "--download_dir", metaVar = "<downloadDirectory>")
	private String downloadDirectory;

	@Option(name = "--timeout", metaVar = "<timeOut>")
	private Integer timeOut = 0;

	@Option(name = "--sleep_interval", metaVar = "<sleepInterval>")
	private Integer sleepInterval = 300; // Defaults to 300 seconds

	private EPFDataDownloadUtil epfDownloadUtil;
	private EPFReleaseUtil epfReleaseUtil;
	private EPFConnector epfConnector;
	private SpazzDBUtil dbUtil;
	private String loggerName;
	private Logger logger;

	/**
	 * Execution method which calls jobInit() to build and link the objects followed by 
	 * epfDownloadUtil.run() to execute the job.
	 * 
	 * @see org.quartz.Job#execute(org.quartz.JobExecutionContext)
	 */
	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		if (context != null) {
			verifyJobDataMap(context);
		}
		jobInit();
		epfDownloadUtil.run();
		logger.info(String.format("Job Completed: %s", this.getClass()
				.getName()));
	}

	/**
	 * Loads and verifies the job settings set in the Quartz Configuration for
	 * this job.
	 * 
	 * @param context
	 *            - JobExectionContext (Quartz Scheduler)
	 */
	public void verifyJobDataMap(JobExecutionContext context) {
		JobDataMap dataMap = context.getJobDetail().getJobDataMap();
		if ((!dataMap.containsKey(DATABASE_HOST))
				|| (!dataMap.containsKey(DATABASE_USER))
				|| (!dataMap.containsKey(DATABASE_PASS))
				|| (!dataMap.containsKey(DATABASE))) {
			throw new RuntimeException(
					"Invalid JobDataMap Database HOST, USER, PASS, DIRECTORY, EPF_DATABASE, EPF_USERNAME and EPF_PASSWORD must be provided");
		}
		host = dataMap.getString(DATABASE_HOST);
		username = dataMap.getString(DATABASE_USER);
		password = dataMap.getString(DATABASE_PASS);
		database = dataMap.getString(DATABASE);
		downloadDirectory = dataMap.getString(DOWNLOAD_DIRECTORY);
		epfUsername = dataMap.getString(EPF_USERNAME);
		epfPassword = dataMap.getString(EPF_PASSWORD);
		timeOut = dataMap.getIntegerFromString(TIMEOUT);
		loggerName = dataMap.getString(LOGGER_NAME);
		sleepInterval = dataMap.getIntegerFromString(SLEEP_INTERVAL);
	}

	/**
	 * Builds and links the objects used by this job including:
	 * <ul>
	 * <li>EPFDataDownloadUtil</li>
	 * <li>EPFReleaseUtil</li>
	 * <li>EPFConnector</li>
	 * <li>SpazzDBUtil</li>
	 * <li>Logger</li>
	 * </ul>
	 * <p>
	 * The main object <i>EPFDataDownloadUtil</i> is executed via it's run()
	 * method.
	 */
	public void jobInit() {
		if (loggerName != null) {
			logger = LoggerFactory.getLogger(loggerName);
		} else {
			logger = LoggerFactory.getLogger(this.getClass());
		}

		logger.info(String
				.format("Job Starting: %s", this.getClass().getName()));

		if (timeOut == 0) {
			timeOut = EPFDataDownloadUtil.DEFAULT_TIMEOUT;
		}

		dbUtil = new SpazzDBUtil(host, database, username, password);
		epfDownloadUtil = new EPFDataDownloadUtil();
		epfReleaseUtil = new EPFReleaseUtil();
		epfConnector = new EPFConnectorImpl();

		epfConnector.setEpfUsername(epfUsername);
		epfConnector.setEpfPassword(epfPassword);
		epfConnector.setEpfBaseUrl(epfBaseUrl);
		epfReleaseUtil.setEpfConnector(epfConnector);
		epfDownloadUtil.setEpfConnector(epfConnector);
		epfDownloadUtil.setEpfUtil(epfReleaseUtil);
		epfDownloadUtil.setLogger(logger);
		epfDownloadUtil.setDbUtil(dbUtil);
		epfDownloadUtil.setDownloadDirectory(downloadDirectory);
		epfDownloadUtil.setSleepInterval(sleepInterval);
		epfDownloadUtil.setTimeOut(timeOut);
	}

	/**
	 * This Quartz Scheduler Job may be run from the command line directly by
	 * providing command line arguments.
	 * 
	 * @param args
	 *            - options for running this Job from the command line
	 */
	public static void main(String[] args) {
		EPFDataDownloadJob job = new EPFDataDownloadJob();
		try {
			job.execute(null);
		} catch (JobExecutionException e) {
			throw new RuntimeException(e);
		}
	}
}
