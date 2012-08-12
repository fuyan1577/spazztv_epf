package com.spazzmania.epf;

import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class EPFReleaseUtilTest {

	private EPFReleaseUtil epfUtil;
	private EPFConnector epfConnector;
	private Date aug012012;
	private Date aug022012;
	private Date aug062012;
	private Date aug072012;
	private Date aug122012;
	private String blackListItem = "match\\d{8}\\.tbz";

	@Before
	public void setUp() throws Exception {
		epfUtil = new EPFReleaseUtil();
		epfConnector = new MockEPFConnector();
		epfUtil.setEpfConnector(epfConnector);
		epfUtil.addBlackListItem(blackListItem);
		DateFormat format = new SimpleDateFormat("dd-MMM-yyyy");
		aug012012 = format.parse("01-AUG-2012");
		aug022012 = format.parse("02-AUG-2012");
		aug062012 = format.parse("06-AUG-2012");
		aug072012 = format.parse("07-AUG-2012");
		aug122012 = format.parse("12-AUG-2012");
	}

	@Test
	public void testUpdateEpfDownloadDates() {
		epfUtil.updateEpfDownloadDates();
		assertTrue("Error updating EPF Date Map", epfUtil.getDateMap().size() > 0);
	}

	@Test
	public void testGetNextDownloadListAug022012() {
		List<String> downloadList = epfUtil.getNextDownloadList(aug012012);
		assertTrue("Error retrieving downloadList",downloadList.size() == 6);
	}
	
	@Test
	public void testGetNextDownloadListAug072012() {
		List<String> downloadList = epfUtil.getNextDownloadList(aug062012);
		assertTrue("Error retrieving downloadList",downloadList.size() == 3);
	}
	
	@Test
	/**
	 * Test for a date not yet available. Should return an empty list.
	 */
	public void testGetNextDownloadListAug122012() {
		List<String> downloadList = epfUtil.getNextDownloadList(aug122012);
		assertTrue("Error retrieving downloadList",downloadList.size() == 0);
	}
}
