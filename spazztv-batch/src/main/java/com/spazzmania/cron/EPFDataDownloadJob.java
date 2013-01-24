/**
 * 
 */
package com.spazzmania.cron;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spazzmania.epf.feed.EPFConnector;
import com.spazzmania.epf.feed.EPFConnectorImpl;
import com.spazzmania.epf.feed.EPFDataDownloadUtil;
import com.spazzmania.epf.feed.EPFFeedException;
import com.spazzmania.epf.feed.EPFReleaseUtil;
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
public class EPFDataDownloadJob {

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

	// Command Line Arguments
	private String username;
	private String password;
	private String database;
	private String host;
	private String epfUsername;
	private String epfPassword;
	private String epfBaseUrl = EPFConnector.EPF_BASE_URL;
	private String downloadDirectory;
	private Integer timeOut = 0;
	private Integer sleepInterval = 300; // Defaults to 300 seconds

	private EPFDataDownloadUtil epfDownloadUtil;
	private EPFReleaseUtil epfReleaseUtil;
	private EPFConnector epfConnector;
	private SpazzDBUtil dbUtil;
	private String loggerName;
	private Logger logger;

	/**
	 * Execution method which calls jobInit() to build and link the objects
	 * followed by epfDownloadUtil.run() to execute the job.
	 * 
	 * @see org.quartz.Job#execute(org.quartz.JobExecutionContext)
	 */
	public void execute() {
		jobInit();
		epfDownloadUtil.run();
		logger.info(String.format("Job Completed: %s", this.getClass()
				.getName()));
	}

	public void parseOptions(String[] args) throws ParseException,
			EPFFeedException {
		Options options = new Options();
		options.addOption("username", true, "database username");
		options.addOption("password", true, "database password");
		options.addOption("database_url", true, "jdbc database url");
		options.addOption("epf_username", true, "EPF Username");
		options.addOption("epf_password", true, "EPF Password");
		options.addOption("epf_baseurl", true, "EPF Base URL");
		options.addOption("download_dir", true, "Download directory");
		options.addOption("timeout", true,
				"Time to wait for availability of files on EPF: defaults to 0 (no waiting)");
		options.addOption("sleep_interval", true,
				"Sleep between retries: defaults to 300 seconds");

		CommandLineParser parser = new PosixParser();
		// Get the command line arguments
		CommandLine line = parser.parse(options, args);

		for (Option option : line.getOptions()) {
			String arg = option.getLongOpt();
			if (arg.equals("database_url")) {
				this.database = line.getOptionValue(arg);
			}
			if (arg.equals("username")) {
				this.username = line.getOptionValue(arg);
			}
			if (arg.equals("password")) {
				this.password = line.getOptionValue(arg);
			}
			if (arg.equals("epf_username")) {
				this.epfUsername = line.getOptionValue(arg);
			}
			if (arg.equals("epf_password")) {
				this.epfPassword = line.getOptionValue(arg);
			}
			if (arg.equals("epf_baseurl")) {
				this.epfBaseUrl = line.getOptionValue(arg);
			}
			if (arg.equals("donwload_dir")) {
				this.downloadDirectory = line.getOptionValue(arg);
			}
			if (arg.equals("timeout")) {
				this.timeOut = Integer.decode(line.getOptionValue(arg));
			}
			if (arg.equals("sleep_interval")) {
				this.sleepInterval = Integer.decode(line.getOptionValue(arg));
			}
		}

		for (String opt : line.getArgs()) {
			if (opt.contains("=")) {
				throw new EPFFeedException(String.format(
						"Unrecognized Option: %s", opt));
			}
		}
	}

//	/**
//	 * Loads and verifies the job settings set in the Quartz Configuration for
//	 * this job.
//	 * 
//	 * @param context
//	 *            - JobExectionContext (Quartz Scheduler)
//	 */
//	public void verifyEpfDbConfig(EPFDbConfig config) {
//		if ((!dataMap.containsKey(DATABASE_HOST))
//				|| (!dataMap.containsKey(DATABASE_USER))
//				|| (!dataMap.containsKey(DATABASE_PASS))
//				|| (!dataMap.containsKey(DATABASE))) {
//			throw new RuntimeException(
//					"Invalid JobDataMap Database HOST, USER, PASS, DIRECTORY, EPF_DATABASE, EPF_USERNAME and EPF_PASSWORD must be provided");
//		}
//		host = dataMap.getString(DATABASE_HOST);
//		username = dataMap.getString(DATABASE_USER);
//		password = dataMap.getString(DATABASE_PASS);
//		database = dataMap.getString(DATABASE);
//		downloadDirectory = dataMap.getString(DOWNLOAD_DIRECTORY);
//		epfUsername = dataMap.getString(EPF_USERNAME);
//		epfPassword = dataMap.getString(EPF_PASSWORD);
//		timeOut = dataMap.getIntegerFromString(TIMEOUT);
//		loggerName = dataMap.getString(LOGGER_NAME);
//		sleepInterval = dataMap.getIntegerFromString(SLEEP_INTERVAL);
//	}

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
		Logger logger = LoggerFactory.getLogger(EPFDataDownloadJob.class);
		try {
			job.parseOptions(args);
			job.execute();
		} catch (ParseException e) {
			logger.error("Error parsing command line");
			logger.error(e.getMessage());
			e.printStackTrace();
		} catch (EPFFeedException e) {
			logger.error("EPF Data Download Exception");
			logger.error(e.getMessage());
		}
	}
}
