package com.spazztv.epf;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.spazztv.epf.dao.EPFDbConfig;
import com.spazztv.epf.dao.EPFDbWriter;
import com.spazztv.epf.dao.EPFDbWriterFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ EPFImportManager.class, EPFFileReader.class,
		EPFImportTranslator.class, EPFDbWriterFactory.class, EPFDbWriter.class,
		EPFImportTask.class, File.class, Executors.class, ExecutorService.class })
public class EPFImportManagerTest {

	private EPFImportManager importManager;
	private EPFDbWriter dbWriter;
	private EPFImportTask importTask1;
	private EPFImportTask importTask2;
	private File file;
	private ExecutorService service;
	private EPFConfig config;
	private EPFDbConfig dbConfig;
	private Future<Runnable> future1;
	private Future<Runnable> future2;
	private List<String> expectedWhiteList;
	private List<String> expectedBlackList;
	private File[] expectedFileList;
	private List<String> expectedFinalList;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		dbWriter = EasyMock.createMock(EPFDbWriter.class);
		importTask1 = EasyMock.createMock(EPFImportTask.class);
		importTask2 = EasyMock.createMock(EPFImportTask.class);
		service = EasyMock.createMock(ExecutorService.class);
		future1 = EasyMock.createMock(Future.class);
		future2 = EasyMock.createMock(Future.class);
		file = EasyMock.createMock(File.class);

		config = new EPFConfig();
		expectedWhiteList = new ArrayList<String>();
		expectedWhiteList.add("^listitem\\d+$");

		expectedBlackList = new ArrayList<String>();
		expectedBlackList.add("listitem3");
		expectedBlackList.add("listitem4");

		expectedFileList = new File[0];
		List<File> tmpFileList = new ArrayList<File>();
		tmpFileList.add(new File("listitem1"));
		tmpFileList.add(new File("listitem2"));
		expectedFileList = tmpFileList.toArray(new File[0]);
		
		expectedFinalList = new ArrayList<String>();
		expectedFinalList.add("listitem1");
		expectedFinalList.add("listitem2");

		config = new EPFConfig();
		config.setAllowExtensions(false);
		config.setBlackList(expectedBlackList);
		config.setWhiteList(expectedWhiteList);
		config.setMaxThreads(8);
		ArrayList<String> dirPaths = new ArrayList<String>();
		dirPaths.add("./");
		config.setDirectoryPaths(dirPaths);

		dbConfig = new EPFDbConfig();
	}

	public void setupEpfConfig() {
		config = new EPFConfig();
	}

	@Test
	public void testEPFImportManager() throws Exception {
		EasyMock.reset(file);
		file.listFiles((FileFilter)EasyMock.anyObject());
		EasyMock.expectLastCall().andReturn(expectedFileList).times(1);
		EasyMock.replay(file);

		EasyMock.reset(service);
		service.submit(importTask1);
		EasyMock.expectLastCall().andReturn(future1).times(1);
		service.submit(importTask2);
		EasyMock.expectLastCall().andReturn(future2).times(1);
		service.shutdown();
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(service);

		EasyMock.reset(future1);
		future1.isDone();
		EasyMock.expectLastCall().andReturn(false).times(1);
		EasyMock.expectLastCall().andReturn(true).times(2);
		EasyMock.replay(future1);

		EasyMock.reset(future2);
		future2.isDone();
		EasyMock.expectLastCall().andReturn(false).times(1);
		EasyMock.expectLastCall().andReturn(true).times(1);
		EasyMock.replay(future2);

		PowerMock.reset(Executors.class);
		PowerMock.mockStatic(Executors.class);
		EasyMock.expect(Executors.newFixedThreadPool(8)).andReturn(service);
		PowerMock.replay(Executors.class);

		PowerMock.reset(EPFDbWriterFactory.class);
		PowerMock.mockStatic(EPFDbWriterFactory.class);
		EasyMock.expect(EPFDbWriterFactory.getDbWriter(dbConfig))
				.andReturn(dbWriter).times(2);
		EPFDbWriterFactory.closeFactory();
		PowerMock.expectLastCall().times(1);
		PowerMock.replay(EPFDbWriterFactory.class);

		PowerMock
				.expectNew(EPFImportTask.class, "listitem1", "&#0002;",
						dbWriter).andReturn(importTask1).times(1);
		PowerMock
				.expectNew(EPFImportTask.class, "listitem2", "&#0002;",
						dbWriter).andReturn(importTask2).times(1);
		PowerMock.replay(EPFImportTask.class);

		PowerMock.expectNew(File.class, config.getDirectoryPaths().get(0))
				.andReturn(file);
		PowerMock.replay(File.class);

		importManager = new EPFImportManager(config, dbConfig);

		Assert.assertTrue("Invalid response: expected isRunning() = true",
				importManager.isRunning());
		Assert.assertTrue("Invalid response: expected isRunning() = true",
				importManager.isRunning());
		Assert.assertFalse("Invalid response: expected isRunning() = false",
				importManager.isRunning());

		PowerMock.verify(EPFDbWriterFactory.class);
		PowerMock.verify(EPFImportTask.class);
		PowerMock.verify(Executors.class);
		PowerMock.verify(File.class);
		EasyMock.verify(service);
		EasyMock.verify(file);
		EasyMock.verify(future1);
		EasyMock.verify(future2);
	}
}
