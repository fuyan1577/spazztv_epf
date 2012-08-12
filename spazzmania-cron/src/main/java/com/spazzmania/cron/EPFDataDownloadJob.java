/**
 * 
 */
package com.spazzmania.cron;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.kohsuke.args4j.Option;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spazzmania.epf.EPFConnector;
import com.spazzmania.epf.EPFConnectorImpl;
import com.spazzmania.epf.EPFDownloadUtil;
import com.spazzmania.model.util.SpazzDBUtil;

/**
 * <b>EPFDataDownloadJob</b> - This download the latest files from Apple's EPF
 * Data Feed. The last download date is retrieved from the database per the job
 * configuration, and the EPF Data Feed which occurs after that date is
 * downloaded.
 * <p>
 * EPF has Full Downloads and Incremental Downloads. The EPF Full Download is
 * done in conjunction with it's first Incremental Download - <i>that's the way
 * Apple is currently doing it!</i>
 * 
 * <p>
 * <i>Note:</i> This program downloads files only, it doesn't run the scripts
 * that apply the data.
 * 
 * @author tjbillingsley
 * 
 */
public class EPFDataDownloadJob implements Job {

	public static String DATABASE_HOST = "database_host";
	public static String DATABASE_USER = "database_user";
	public static String DATABASE_PASS = "database_pass";
	public static String DATABASE = "database";
	public static String DOWNLOAD_DIRECTORY = "download_directory";
	public static String TIMEOUT = "timeout";
	public static Integer DEFAULT_TIMEOUT = 720; // 720 minutes or 12 hours
	public static String LOGGER_NAME = "logger_name";

	public static String SLEEP_INTERVAL = "sleep_interval";
	public static String EPF_LAST_DOWNLOAD_DATE = "epf.last_download_date";

	@Option(name = "-u", aliases = "--username", metaVar = "<username>")
	private String username;

	@Option(name = "-p", aliases = "--password", metaVar = "<password>")
	private String password;

	@Option(name = "-e", aliases = "--epf_database", metaVar = "<database>")
	private String database;

	@Option(name = "-h", aliases = "--host", metaVar = "<host>")
	private String host;

	@Option(name = "-d", aliases = "--download_dir", metaVar = "<downloadDirectory>")
	private String downloadDirectory;

	@Option(name = "-t", aliases = "--timeout", metaVar = "<timeOut>")
	private Integer timeOut = 0;

	@Option(name = "-s", aliases = "--sleep_interval", metaVar = "<sleepInterval>")
	private Integer sleepInterval = 300; // Defaults to 300 seconds

	private EPFDownloadUtil epfUtil;
	private EPFConnector epfConnector;
	private SpazzDBUtil dbUtil;
	private boolean epfDownloadFilesReady = false;
	private List<String> epfDownloadFileList;
	Date lastDownloadDate;
	Date startTime;
	Date timeOutTime;

	String loggerName;
	Logger logger;

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.quartz.Job#execute(org.quartz.JobExecutionContext)
	 */
	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		if (context != null) {
			verifyJobDataMap(context);
		}

		jobSetup();
		waitForDownloadFiles();
		if (epfDownloadFilesReady) {
			downloadEpfFiles();
		}
	}

	public void jobSetup() {

		if (loggerName != null) {
			logger = LoggerFactory.getLogger(loggerName);
		} else {
			logger = LoggerFactory.getLogger(this.getClass());
		}

		logger.info(String
				.format("Job Starting: %s", this.getClass().getName()));

		if (timeOut == 0) {
			timeOut = DEFAULT_TIMEOUT;
		}

		dbUtil = new SpazzDBUtil(host, database, username, password);
		epfUtil = new EPFDownloadUtil();
		epfConnector = new EPFConnectorImpl();
		epfUtil.setEpfConnector(epfConnector);

		DateFormat dateXlater = new SimpleDateFormat("yyyyMMdd");
		String lastDateValue = dbUtil.getKeyValue(EPF_LAST_DOWNLOAD_DATE);
		if (lastDateValue != null) {
			try {
				lastDownloadDate = dateXlater.parse(lastDateValue);
			} catch (ParseException e) {
			}
		}

		Calendar startDateTime = Calendar.getInstance();
		startTime = new Date(startDateTime.getTimeInMillis());
		startDateTime.add(Calendar.MINUTE, timeOut);
		timeOutTime = new Date(startDateTime.getTimeInMillis());

		// If we still don't have a date, use yesterday's date and get the file
		// for today
		if (lastDownloadDate == null) {
			// Subtract 1 day from today
			startDateTime.setTime(startTime);
			startDateTime.add(Calendar.DATE, -1);
			// Set last download date to yesterday
			lastDownloadDate = new Date(startDateTime.getTimeInMillis());
		}

		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm");
		logger.info("Last Download Date: "
				+ dateXlater.format(lastDownloadDate));
		logger.info("Job will check for files date/time: "
				+ formatter.format(timeOutTime));
	}

	public void waitForDownloadFiles() {
		epfDownloadFilesReady = false;

		while (epfDownloadFilesReady == false) {
			epfDownloadFileList = epfUtil.getNextDownloadList(lastDownloadDate);
			if (epfDownloadFileList.size() == 0) {
				// Too early - we'll have to sleep and try again
				if (jobTimedOut()) {
					return;
				}
				try {
					Thread.sleep(sleepInterval * 1000);
				} catch (InterruptedException e) {
				}
			} else {
				epfDownloadFilesReady = true;
				break;
			}
		}
	}

	public boolean jobTimedOut() {
		Calendar currentTime = Calendar.getInstance();
		return currentTime.getTimeInMillis() >= timeOutTime.getTime();
	}

	public void downloadEpfFiles() {
		for (String downloadUrl : epfDownloadFileList) {
			epfConnector.downloadFileFromUrl(downloadUrl, downloadDirectory);
		}
	}

	private void verifyJobDataMap(JobExecutionContext context) {
		JobDataMap dataMap = context.getJobDetail().getJobDataMap();
		if ((!dataMap.containsKey(DATABASE_HOST))
				|| (!dataMap.containsKey(DATABASE_USER))
				|| (!dataMap.containsKey(DATABASE_PASS))
				|| (!dataMap.containsKey(DATABASE))) {
			throw new RuntimeException(
					"Invalid JobDataMap Database HOST, USER, PASS, DIRECTORY and EPF_DATABASE must be provided");
		}
		host = (String) dataMap.get(DATABASE_HOST);
		username = (String) dataMap.get(DATABASE_USER);
		password = (String) dataMap.get(DATABASE_PASS);
		database = (String) dataMap.get(DATABASE);
		downloadDirectory = (String) dataMap.get(DOWNLOAD_DIRECTORY);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		EPFDataDownloadJob job = new EPFDataDownloadJob();
		try {
			job.execute(null);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
