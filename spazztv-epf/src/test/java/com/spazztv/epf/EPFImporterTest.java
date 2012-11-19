package com.spazztv.epf;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.cli.ParseException;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.spazztv.epf.dao.EPFDbConfig;
import com.spazztv.epf.dao.EPFDbConnector;
import com.spazztv.epf.dao.EPFDbWriter;
import com.spazztv.epf.dao.EPFDbWriterFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ EPFImportManager.class, EPFImporter.class, EPFDbWriterFactory.class })
public class EPFImporterTest {
	
	private EPFImporter epfImporter;
	
	@Before
	public void setUp() throws Exception {
		epfImporter = new EPFImporter();
	}

	@Test
	public void testMainWhiteList() throws ParseException, IOException, EPFImporterException {
		String[] args = {"--config","testdata/EPFConfig.json", "-w", "\"whitelist1\"","-w","whitelist2","--whitelist","'whitelist3'","-w","\"whitelist4\"","-b","blacklist1","--blacklist","blacklist2"};

		epfImporter.parseCommandLine(args);
		
		int expectedCount = 4;
		List<String>actualWhiteList = epfImporter.getConfig().getWhiteList();
		Assert.assertTrue("Invalid whiteList parsed - no values parsed",actualWhiteList != null);
		Assert.assertTrue(String.format("Invalid whitelist parsing, expecting %d whitelist items, actual %d",expectedCount,actualWhiteList.size()),actualWhiteList.size() == expectedCount);
	}

	@Test
	public void testMainBlacklist() throws ParseException, IOException, EPFImporterException {
		String[] args = {"-config","testdata/EPFConfig.json", "-b","blacklist1","--blacklist","blacklist2"};
		
		epfImporter.parseCommandLine(args);
		
		int expectedCount = 2;
		List<String>actualBlackList = epfImporter.getConfig().getBlackList();
		Assert.assertTrue("Invalid whiteList parsed - no values parsed",actualBlackList != null);
		Assert.assertTrue(String.format("Invalid whitelist parsing, expecting %d whitelist items, actual %d",expectedCount,actualBlackList.size()),actualBlackList.size() == expectedCount);
	}
	
	@Test
	public void testRunImporterJob() throws Exception {
		EPFImportManager importManager = EasyMock.createMock(EPFImportManager.class);
		importManager.isRunning();
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.expectLastCall().andReturn(false).times(1);
		EasyMock.replay(importManager);
		
		EPFConfig config = new EPFConfig();
		EPFDbConfig dbConfig = new EPFDbConfig();
		
		PowerMock.expectNew(EPFImportManager.class,config,dbConfig).andReturn(importManager).times(1);
		PowerMock.replay(EPFImportManager.class);
		
		epfImporter.setConfig(config);
		epfImporter.setDbConfig(dbConfig);
		epfImporter.setPauseInterval(1000);
		epfImporter.runImporterJob();
		
		PowerMock.verify(EPFImportManager.class);
		EasyMock.verify(importManager);
	}
}
