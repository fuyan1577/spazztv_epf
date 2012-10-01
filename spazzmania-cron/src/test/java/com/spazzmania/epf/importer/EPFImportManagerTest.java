package com.spazzmania.epf.importer;

import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.spazzmania.epf.dao.EPFDbConfig;
import com.spazzmania.epf.dao.EPFDbWriter;
import com.spazzmania.epf.dao.EPFDbWriterFactory;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ EPFImportManager.class, EPFFileReader.class,
		EPFImportTranslator.class, EPFDbWriter.class, EPFImportTask.class,
		File.class, Executors.class, ExecutorService.class})
public class EPFImportManagerTest {
	
	private EPFImportManager importManager;
	private EPFFileReader fileReader;
	private EPFImportTranslator importTranslator;
	private EPFDbWriter dbWriter;
	private EPFImportTask importTask;
	private File file;
	private ExecutorService service;
	private EPFConfig config;
	private EPFDbConfig dbConfig;
	private Future future;

	@Before
	public void setUp() throws Exception {
		fileReader = EasyMock.createMock(EPFFileReader.class);
		importTranslator = EasyMock.createMock(EPFImportTranslator.class);
		dbWriter = EasyMock.createMock(EPFDbWriter.class);
		importTask = EasyMock.createMock(EPFImportTask.class);
		service= EasyMock.createMock(ExecutorService.class);
		future = EasyMock.createMock(Future.class);
	}

	@Test
	public void testEPFImportManager() throws Exception {
		
		List<String>fileList = new ArrayList<String>();
		fileList.add("whitelist1");
		fileList.add("whitelist2");

		PowerMock.reset(Executors.class);
		PowerMock.mockStatic(Executors.class);
		EasyMock.expect(Executors.newFixedThreadPool(8)).andReturn(service);
		PowerMock.replay(Executors.class);

		PowerMock.reset(EPFDbWriterFactory.class);
		PowerMock.mockStatic(EPFDbWriterFactory.class);
		EasyMock.expect(EPFDbWriterFactory.getDbWriter(dbConfig)).andReturn(dbWriter);
		PowerMock.replay(EPFDbWriterFactory.class);
	
		PowerMock.expectNew(EPFFileReader.class, "whitelist1").andReturn(fileReader);
		PowerMock.replay(EPFFileReader.class);
		
		PowerMock.expectNew(EPFFileReader.class, "whitelist2").andReturn(fileReader);
		PowerMock.replay(EPFFileReader.class);
		
		PowerMock.expectNew(EPFImportTranslator.class,fileReader).andReturn(importTranslator);
		PowerMock.replay(EPFImportTranslator.class);
		
		PowerMock.expectNew(EPFImportTask.class,importTranslator,dbWriter).andReturn(importTask);
		PowerMock.replay(EPFImportTask.class);
		
		EasyMock.reset(service);
		service.submit(importTask);
		EasyMock.expectLastCall().times(1).andReturn(future);
		EasyMock.replay(service);
		
		
//		EPFImportTranslator importTranslator = new EPFImportTranslator(fileReader);
//		EPFDbWriter dbWriter = EPFDbWriterFactory.getDbWriter(dbConfig);
//		EPFImportTask importTask = new EPFImportTask(importTranslator, dbWriter);
//		importThreads.add((Future<Runnable>) service.submit(importTask));
	}

	@Test
	public void testLoadImportFileList() {
		fail("Not yet implemented");
	}
	
	@Test
	public void testIsValidImportFile() {
		fail("Not yet implemented");
	}

	@Test
	public void testLoadImportThreads() {
		fail("Not yet implemented");
	}

	@Test
	public void testIsRunning() {
		fail("Not yet implemented");
	}

}
