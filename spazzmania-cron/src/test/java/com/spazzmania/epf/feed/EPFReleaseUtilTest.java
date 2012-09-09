package com.spazzmania.epf.feed;

import static org.junit.Assert.assertTrue;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.spazzmania.epf.feed.EPFConnector;
import com.spazzmania.epf.feed.EPFReleaseUtil;

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
		assertTrue("Error updating EPF Date Map",
				epfUtil.getDateMap().size() > 0);
	}

	@Test
	public void testGetNextDownloadListAug022012() {
		List<String> downloadList = epfUtil.getNextDownloadList(aug012012);
		assertTrue("Error retrieving downloadList", downloadList.size() == 6);
		validateDownloadUrls(downloadList);

	}

	@Test
	public void testGetNextDownloadListAug072012() {
		List<String> downloadList = epfUtil.getNextDownloadList(aug062012);
		assertTrue("Error retrieving downloadList", downloadList.size() == 3);
		validateDownloadUrls(downloadList);

	}

	@Test
	public void testGetNextDownloadListAug082012() {
		EPFConnector connector = EasyMock.createMock(EPFConnector.class);
		String fullUrl = "/";
		String incDateAug01Url = "/20120801/incremental";
		String incDateAug08Url = "/20120808/incremental";
		String incFileUrl = "/20120801/incremental/20120808";

		connector.getEpfPage(fullUrl);
		EasyMock.expectLastCall().andReturn(epfConnector.getEpfPage(fullUrl))
				.times(1);

		connector.getEpfPage(incDateAug01Url);
		EasyMock.expectLastCall()
				.andReturn(epfConnector.getEpfPage(incDateAug01Url)).times(1);

		connector.getEpfPage(incDateAug08Url);
		EasyMock.expectLastCall()
				.andReturn(epfConnector.getEpfPage(incDateAug08Url)).times(1);

		connector.getEpfPage(incFileUrl);
		EasyMock.expectLastCall()
				.andReturn(epfConnector.getEpfPage(incFileUrl)).times(1);

		EasyMock.replay(connector);

		epfUtil.setEpfConnector(connector);

		List<String> downloadList = epfUtil.getNextDownloadList(aug072012);
		assertTrue("Error retrieving downloadList", downloadList.size() == 3);
		
		validateDownloadUrls(downloadList);

		EasyMock.verify(connector);
	}

	public void validateDownloadUrls(List<String> downloadList) {
		for (String downloadUrl : downloadList) {
			assertTrue(String.format("Invalid file URL: %s", downloadUrl),
					isValidDownloadUrl(downloadUrl));
		}
	}
	
	public boolean isValidDownloadUrl(String downloadUrl) {
		boolean fullPath = Pattern.matches(".+/full/\\d{8}/[a-z]+\\d{8}\\.tbz", downloadUrl);
		boolean incPath = Pattern.matches(".+/full/\\d{8}/incremental/\\d{8}/[a-z]+\\d{8}\\.tbz", downloadUrl);
		return fullPath || incPath;
	}

	@Test
	/**
	 * Test for a date not yet available. Should return an empty list.
	 */
	public void testGetNextDownloadListAug122012() {
		List<String> downloadList = epfUtil.getNextDownloadList(aug122012);
		assertTrue("Error retrieving downloadList", downloadList.size() == 0);
	}
}
