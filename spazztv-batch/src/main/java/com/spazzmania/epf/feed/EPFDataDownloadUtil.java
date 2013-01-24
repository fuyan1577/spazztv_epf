/**
 * 
 */
package com.spazzmania.epf.feed;

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
 * @author Thomas Billingsley
 * 
 */
public class EPFDataDownloadUtil {

	public static Integer DEFAULT_TIMEOUT = 12 * 60 * 60; // defaults to 12hrs
	public static String EPF_LAST_DOWNLOAD_DATE = "epf.last_epf_import_date";

	private String downloadDirectory;
	private Integer timeOut = DEFAULT_TIMEOUT;
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

	/**
	 * Set the directory path where the EPF files will be downloaded.
	 * 
	 * @param timeOut
	 *            - time in minutes
	 */
	public void setDownloadDirectory(String downloadDirectory) {
		this.downloadDirectory = downloadDirectory;
	}

	public Integer getTimeOut() {
		return timeOut;
	}

	/**
	 * Set the timeout time in seconds for how long this utility waits for EPF
	 * to publish the next set of files.
	 * 
	 * @param timeOut
	 *            - time in seconds
	 */
	public void setTimeOut(Integer timeOut) {
		this.timeOut = timeOut;
	}

	public Integer getSleepInterval() {
		return sleepInterval;
	}

	/**
	 * Returns true if the checkAndWaitForDownloadFiles successfully returned a
	 * list of EPF Files.
	 * 
	 * @return the epfDownloadFilesReady
	 */
	public boolean isEpfDownloadFilesReady() {
		return epfDownloadFilesReady;
	}

	/**
	 * Returns the list of EPF File URLs representing the next set of files past
	 * the last EPF Import Date in the Spazzmania DB.
	 * 
	 * @return the epfDownloadFileList
	 */
	public List<String> getEpfDownloadFileList() {
		return epfDownloadFileList;
	}

	/**
	 * Set the sleep interval in seconds between EPF Site lookups. An <i>EPF
	 * Site Lookup</i> is done each time this program checks the availability of
	 * the next EPF Files.
	 * 
	 * @param sleepInterval
	 *            - in seconds
	 */
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

	/**
	 * Run the utility - this is the main execution point for running this
	 * utility. The steps are:
	 * <ul>
	 * <li/>jobInit() - get the last_download_date from the SpazzDB, set the
	 * timeout time, etc.
	 * <li/>checkAndWaitForDownloadFiles() - Check and Wait for the next
	 * download files
	 * <li/>downloadEpfFiles() - If we haven't timed out, download the files
	 * </ul>
	 */
	public void run() {
		jobInit();
		checkAndWaitForDownloadFiles();
		if (epfDownloadFilesReady) {
			downloadEpfFiles();
		}
	}

	/**
	 * The jobInit() routine retrieves the last_download_date from the
	 * Spazzmania DB, and calculates the time that this utility will timeout
	 * based on the timeOut value.
	 */
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
		startDateTime.add(Calendar.SECOND, timeOut);
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

	/**
	 * Check for and possibly wait for the next set of EPF Files. Logic:
	 * <ul>
	 * <li/>Check the EPF site if the next set of EPF Files are ready
	 * <li/>If they are, return the list of file URLs
	 * <li/>If they aren't ready, repeat the process
	 * <li/>This process times out based on the timeOut property
	 * </ul>
	 */
	public void checkAndWaitForDownloadFiles() {
		epfDownloadFilesReady = false;

		while (epfDownloadFilesReady == false) {
			logger.info("Checking EPF for the next set of Download Files");
			epfDownloadFileList = epfUtil.getNextDownloadList(lastDownloadDate);
			if (epfDownloadFileList.size() == 0) {
				// Too early - we'll have to sleep and try again
				if (isJobTimedOut()) {
					return;
				}
				logger.info(String.format(
						"EPF Files Not Ready. Sleepting %s seconds",
						sleepInterval.toString()));
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

	/**
	 * Convenience method for annotation to check if the routine has timed out.
	 * 
	 * @return - true if timed out
	 */
	public boolean isJobTimedOut() {
		Calendar currentTime = Calendar.getInstance();
		return currentTime.getTimeInMillis() >= timeOutTime.getTime();
	}

	/**
	 * Download the files in the <code>epfDownloadFileList</code> to the
	 * directory designated in <code>downloadDirectory</code>.
	 */
	public void downloadEpfFiles() {
		for (String downloadUrl : epfDownloadFileList) {
			epfConnector.downloadFileFromUrl(downloadUrl, downloadDirectory);
		}
	}
}
