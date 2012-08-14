/**
 * Copyright (c) 2012, Spazzmania, Inc. All rights reserved.
 */
package com.spazzmania.epf;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

/**
 * The class <code>EPFDownloadUtil</code> is a utility object which returns a
 * list of EPF download files released since a given date. This object is
 * dependent on an <code>EPFConnector</code> implementation which is used to get
 * listings of released EPF files.
 * <p>
 * The only methods intended for external use are
 * <code>updateEpfDownloadDate()</code> and
 * <code>getNextDownloadList(Date)</code>. The method <code>getDateMap()</code>
 * is public for JUnit testing only.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFReleaseUtil {
	private static int EPF_FULL = 0;
	private static int EPF_INCREMENTAL = 1;
	private boolean requiresEpfFull = false;
	private TreeMap<Date, List<Date>> dateMap;
	private EPFConnector epfConnector;
	private List<String> blackList = new ArrayList<String>();

	public void setEpfConnector(EPFConnector epfConnector) {
		this.epfConnector = epfConnector;
	}

	public void addBlackListItem(String blackListItem) {
		blackList.add(blackListItem);
	}

	/**
	 * Returns the Date map of full release and incremental EPF Data Releases.
	 * The method <code>updateEpfDownloadDate</code> should be called first
	 * before retrieving the map.
	 * <p>
	 * <i>Note:</i> This method was made public primarily for JUnit Testing.
	 * 
	 * @return an EPF Date Map of available EPF Releases
	 */
	public Map<Date, List<Date>> getDateMap() {
		return dateMap;
	}

	/**
	 * This method returns a list of the next EPF download files after the
	 * lastDownloadDate.
	 * <p>
	 * 
	 * @param lastDownloadDate
	 *            - Used to determine the next set of download files.
	 * @return - A List of Download file URLs
	 */
	public List<String> getNextDownloadList(Date lastDownloadDate) {
		updateEpfDownloadDates();
		
		Date[] epfDates;
		DateFormat dateParser = new SimpleDateFormat("yyyyMMdd");

		List<String> downloadList = new ArrayList<String>();

		epfDates = getNextEpfDownloadDate(lastDownloadDate);
		if ((epfDates[0] == null) || (epfDates[1] == null)) {
			return downloadList;
		}

		String epfFullPath = "/" + dateParser.format(epfDates[EPF_FULL]);
		String epfIncrementalPath = epfFullPath + "/incremental/"
				+ dateParser.format(epfDates[EPF_INCREMENTAL]);

		if (requiresEpfFull) {
			downloadList.addAll(getEpfDownloadFiles(epfFullPath));
		}
		downloadList.addAll(getEpfDownloadFiles(epfIncrementalPath));

		return downloadList;
	}

	/**
	 * This method updates the internal list of EPF Download file Release Dates.
	 * After calling this method, a call to <code>getDateMap()</code> will
	 * return the date list.
	 * <p>
	 * <i>Note:</i> This method was made <i>public</i> primarily for JUnit
	 * testing purposes.
	 * <p>
	 * 
	 * @param lastDownloadDate
	 *            - Used to determine the next set of download files.
	 * @return - A List of Download file URLs
	 */
	public void updateEpfDownloadDates() {
		DateFormat dateXlate = new SimpleDateFormat("yyyyMMdd");

		TreeMap<Date, List<Date>> dateList = new TreeMap<Date, List<Date>>();

		for (Date nextFullDate : getEpfDateList("/")) {
			List<Date> subList = new ArrayList<Date>();
			for (Date nextIncDate : getEpfDateList("/"
					+ dateXlate.format(nextFullDate) + "/incremental")) {
				subList.add(nextIncDate);
			}

			dateList.put(nextFullDate, subList);
		}

		this.dateMap = dateList;
	}

	private Date[] getNextEpfDownloadDate(Date lastDownloadDate) {
		Date[] epfDates = new Date[2];
		for (Map.Entry<Date, List<Date>> entry : dateMap.entrySet()) {
			requiresEpfFull = true;
			for (Date incDate : entry.getValue()) {
				if (incDate.after(lastDownloadDate)) {
					epfDates[EPF_FULL] = entry.getKey();
					epfDates[EPF_INCREMENTAL] = incDate;
					return epfDates;
				}
				requiresEpfFull = false;
			}
		}
		return epfDates;
	}

	private List<Date> getEpfDateList(String epfPath) {
		List<Date> dateList = new ArrayList<Date>();

		DateFormat dateParser = new SimpleDateFormat("yyyyMMdd");
		Document document = epfConnector.getEpfPage(epfPath);
		if (document == null) {
			return dateList;
		}

		Object[] hrefs = document.select("a").toArray();
		for (Object href : hrefs) {
			String h = ((Element) href).attr("href");
			if (Pattern.matches("\\d{8}\\/", h)) {
				try {
					dateList.add(dateParser.parse(h));
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
		return dateList;
	}

	private List<String> getEpfDownloadFiles(String epfPath) {
		List<String> downloadFiles = new ArrayList<String>();

		Document document = epfConnector.getEpfPage(epfPath);
		if (document == null) {
			return downloadFiles;
		}

		Object[] hrefs = document.select("a").toArray();
		for (Object href : hrefs) {
			String h = ((Element) href).attr("href");
			if (Pattern.matches("[a-z]+\\d{8}\\.tbz", h)) {
				if (!isBlackListedItem(h)) {
					downloadFiles.add(EPFConnector.EPF_BASE_URL + epfPath + "/" + h);
				}
			}
		}

		return downloadFiles;
	}

	private boolean isBlackListedItem(String item) {
		for (String blackListItem : blackList) {
			if (Pattern.matches(".*" + blackListItem + ".*", item)) {
				return true;
			}
		}
		return false;
	}

}