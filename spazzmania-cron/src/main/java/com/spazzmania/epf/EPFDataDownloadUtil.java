/**
 * 
 */
package com.spazzmania.epf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;

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
public class EPFDataDownloadUtil {

	public static Integer DEFAULT_TIMEOUT = 720; // 720 minutes or 12 hours
	public static String EPF_LAST_DOWNLOAD_DATE = "epf.last_download_date";

	private String downloadDirectory;
	private Integer timeOut = 0;
	private Integer sleepInterval = 300; // Defaults to 300 seconds
	private EPFReleaseUtil epfUtil;
	private EPFConnector epfConnector;
	private SpazzDBUtil dbUtil;
	private boolean epfDownloadFilesReady = false;
	private List<String> epfDownloadFileList;
	Date lastDownloadDate;
	Date startTime;
	Date timeOutTime;

	Logger logger;
	
	public SpazzDBUtil getDbUtil() {
		return dbUtil;
	}

	public void setDbUtil(SpazzDBUtil dbUtil) {
		this.dbUtil = dbUtil;
	}

	public String getDownloadDirectory() {
		return downloadDirectory;
	}

	public void setDownloadDirectory(String downloadDirectory) {
		this.downloadDirectory = downloadDirectory;
	}

	public Integer getTimeOut() {
		return timeOut;
	}

	public void setTimeOut(Integer timeOut) {
		this.timeOut = timeOut;
	}

	public Integer getSleepInterval() {
		return sleepInterval;
	}

	public void setSleepInterval(Integer sleepInterval) {
		this.sleepInterval = sleepInterval;
	}

	public EPFReleaseUtil getEpfUtil() {
		return epfUtil;
	}

	public void setEpfUtil(EPFReleaseUtil epfUtil) {
		this.epfUtil = epfUtil;
	}

	public EPFConnector getEpfConnector() {
		return epfConnector;
	}

	public void setEpfConnector(EPFConnector epfConnector) {
		this.epfConnector = epfConnector;
	}

	public Logger getLogger() {
		return logger;
	}

	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	public void run() {
		jobInit();
		waitForDownloadFiles();
		if (epfDownloadFilesReady) {
			downloadEpfFiles();
		}
	}

	public void jobInit() {
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
}
