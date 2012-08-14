package com.spazzmania.epf;

import static org.junit.Assert.fail;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import com.spazzmania.model.util.SpazzDBUtil;

public class EPFDataDownloadUtilTest {
	
	private EPFDataDownloadUtil epfDownloadUtil;
	private EPFReleaseUtil epfUtil;
	private EPFConnector epfConnector;
	private SpazzDBUtil dbUtil;
	private Logger logger;
	private String expectedDownloadDateStr = "20120801";
	private Date expectedDownloadDate;
	private String downloadDir;
	
	@Before
	public void setUp() throws Exception {
		epfConnector = EasyMock.createMock(EPFConnector.class);
		dbUtil = EasyMock.createMock(SpazzDBUtil.class);
		logger = EasyMock.createMock(Logger.class);
		epfUtil = EasyMock.createMock(EPFReleaseUtil.class);

		logger.info(EasyMock.anyObject(String.class));
		EasyMock.expectLastCall().anyTimes();
		EasyMock.replay(logger);

		DateFormat dateXlater = new SimpleDateFormat("yyyyMMdd");
		expectedDownloadDate = dateXlater.parse(expectedDownloadDateStr);
		
		downloadDir = "/test/download/dir/";
		
		epfDownloadUtil = new EPFDataDownloadUtil();
		epfDownloadUtil.setDbUtil(dbUtil);
		epfDownloadUtil.setEpfConnector(epfConnector);
		epfDownloadUtil.setEpfUtil(epfUtil);
		epfDownloadUtil.setLogger(logger);
		epfDownloadUtil.setDownloadDirectory(downloadDir);
	}

	@Test
	public void testJobInit() {
		dbUtil.getKeyValue(EPFDataDownloadUtil.EPF_LAST_DOWNLOAD_DATE);
		EasyMock.expectLastCall().andReturn(expectedDownloadDateStr).times(1);
		EasyMock.replay(dbUtil);

		epfDownloadUtil.setTimeOut(1);
		epfDownloadUtil.jobInit();
		
		try {
			Thread.sleep(2 * 1000);
		} catch (InterruptedException e) {
		}
		
		EasyMock.verify(dbUtil);
		Assert.assertTrue("Job should have timed out",epfDownloadUtil.isJobTimedOut());
	}

	@Test
	public void testCheckAndWaitForDownloadFiles() {
		List<String>emptyList = new ArrayList<String>();
		epfUtil.getNextDownloadList(expectedDownloadDate);
		EasyMock.expectLastCall().andReturn(emptyList).times(1);
		EasyMock.replay(epfUtil);
		dbUtil.getKeyValue(EPFDataDownloadUtil.EPF_LAST_DOWNLOAD_DATE);
		EasyMock.expectLastCall().andReturn(expectedDownloadDateStr).times(1);
		EasyMock.replay(dbUtil);
		
		epfDownloadUtil.setTimeOut(0);
		epfDownloadUtil.setSleepInterval(2);
		epfDownloadUtil.jobInit();
		epfDownloadUtil.checkAndWaitForDownloadFiles();
		
		EasyMock.verify(dbUtil);
		EasyMock.verify(epfUtil);
		Assert.assertTrue("Download files should be empty", epfDownloadUtil.isJobTimedOut());
	}

	@Test
	public void testCheckAndWaitForDownloadFiles2() {
		List<String>simpleList = new ArrayList<String>();
		
		simpleList.add("file1.tbz");
		simpleList.add("file2.tbz");
		simpleList.add("file3.tbz");
		
		epfUtil.getNextDownloadList(expectedDownloadDate);
		EasyMock.expectLastCall().andReturn(simpleList).times(1);
		EasyMock.replay(epfUtil);
		dbUtil.getKeyValue(EPFDataDownloadUtil.EPF_LAST_DOWNLOAD_DATE);
		EasyMock.expectLastCall().andReturn(expectedDownloadDateStr).times(1);
		EasyMock.replay(dbUtil);
		
		epfDownloadUtil.setTimeOut(0);
		epfDownloadUtil.setSleepInterval(2);
		epfDownloadUtil.jobInit();
		epfDownloadUtil.checkAndWaitForDownloadFiles();

		List<String> returnList = epfDownloadUtil.getEpfDownloadFileList();
		
		EasyMock.verify(dbUtil);
		EasyMock.verify(epfUtil);
		Assert.assertTrue("Download files should be ready", epfDownloadUtil.isEpfDownloadFilesReady());
		Assert.assertTrue("Download files should have 3 entries", returnList.size() == 3);
	}

	@Test
	public void testRun() {
		List<String>simpleList = new ArrayList<String>();
		
		simpleList.add("file1.tbz");
		simpleList.add("file2.tbz");
		simpleList.add("file3.tbz");
		
		epfUtil.getNextDownloadList(expectedDownloadDate);
		EasyMock.expectLastCall().andReturn(simpleList).times(1);
		EasyMock.replay(epfUtil);
		
		dbUtil.getKeyValue(EPFDataDownloadUtil.EPF_LAST_DOWNLOAD_DATE);
		EasyMock.expectLastCall().andReturn(expectedDownloadDateStr).times(1);
		EasyMock.replay(dbUtil);
		
		//epfConnector.downloadFileFromUrl(EasyMock.anyObject(String.class), EasyMock.anyObject(String.class));
		for (String downloadUrl : simpleList) {
			epfConnector.downloadFileFromUrl(downloadUrl, downloadDir);	
			EasyMock.expectLastCall().times(1);
		}
		EasyMock.replay(epfConnector);
		
		epfDownloadUtil.setTimeOut(0);
		epfDownloadUtil.setSleepInterval(2);
		epfDownloadUtil.run();

		List<String>returnList = epfDownloadUtil.getEpfDownloadFileList();
		
		EasyMock.verify(dbUtil);
		EasyMock.verify(epfUtil);
		EasyMock.verify(epfConnector);
		Assert.assertTrue("Download files should be ready", epfDownloadUtil.isEpfDownloadFilesReady());
		Assert.assertTrue("Download files should have 3 entries", returnList.size() == 3);
	}
}
