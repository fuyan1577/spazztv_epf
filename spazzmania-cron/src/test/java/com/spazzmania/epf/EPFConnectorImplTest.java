package com.spazzmania.epf;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

/**
 * EPFConnectorImplTest - This is a End to End String test of the
 * EPFConnectorImpl, an object which download data from Apple's EPF site.
 * 
 * <p>For build testing, the String test methods were commented
 * out. Only the <code>getDownloadFilename()</code> was left uncommented.
 * 
 * @author tjbillingsley
 * 
 */
public class EPFConnectorImplTest {

	private EPFConnectorImpl epfConnector;
	private String testDownloadURL;
	private String testFilename;

	@Before
	public void setUp() throws Exception {
		epfConnector = new EPFConnectorImpl();
		testDownloadURL = "http://feeds.itunes.apple.com/feeds/epf/v3/full/20120808/popularity20120808.tbz";
		testFilename = "popularity20120808.tbz";
	}

	@Test
	public void testDownloadUrl() {
		// This is a string test and should be uncommented only when running the
		// test
		// epfConnector.setShowProgress(true);
		// epfConnector.downloadFileFromUrl(testDownloadURL, "target/");
		assertTrue("String testing object only - returning true", true);
	}

	@Test
	public void testGetDownloadFilename() {
		String filename = epfConnector.getDownloadFilename(testDownloadURL);
		assertTrue(String.format("Expecting %s, filename returned was %s",
				testFilename, filename), filename.equals(testFilename));
	}
}
