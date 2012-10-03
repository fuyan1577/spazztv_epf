package com.spazzmania.epf.importer;

import java.io.FileNotFoundException;
import java.util.LinkedHashMap;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.spazzmania.epf.dao.EPFDbException;
import com.spazzmania.epf.dao.EPFDbWriter;

public class EPFImportTaskTest {

	String storefrontEpfFile = "testdata/epf_files/storefront";
	EPFImportTask importTask;
	EPFImportTranslator importXlator;
	EPFDbWriter dbWriter;
	long recordsExpected = 0;
	
	EPFExportType exportType;
	String tableName;
	LinkedHashMap<String,String> columnsAndTypes;
	
	@Before
	public void setUp() throws FileNotFoundException, EPFFileFormatException {
		//Setting up importXlator to read the storefront data file
		//Setting up dbWriter as a mock object
		EPFFileReader fileReader = new EPFFileReader(storefrontEpfFile);
		dbWriter = EasyMock.createMock(EPFDbWriter.class);
		
		importTask = new EPFImportTask(storefrontEpfFile,dbWriter);
		
		importXlator = new EPFImportTranslator(fileReader);
		recordsExpected = importXlator.getTotalDataRecords();
		exportType = importXlator.getExportType();
		tableName = importXlator.getTableName();
		columnsAndTypes = importXlator.getColumnAndTypes();
	}

//	dbWriter.initImport(importXlator.getExportType(),
//	importXlator.getTableName(), importXlator.getColumnAndTypes(),
//	importXlator.getTotalDataRecords());
	@Test
	public void testSetupImportDataStore() throws EPFDbException {
		EasyMock.reset(dbWriter);
		dbWriter.initImport(exportType, tableName, columnsAndTypes, recordsExpected);
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(dbWriter);
		importTask.setupImportDataStore();
		EasyMock.verify(dbWriter);
	}

	@Test
	public void testImportData() throws EPFDbException  {
		EasyMock.reset(dbWriter);
		dbWriter.insertRow(EasyMock.anyObject(String[].class));
		EasyMock.expectLastCall().times((int)importXlator.getTotalDataRecords());
		EasyMock.replay(dbWriter);
		importTask.importData();
		EasyMock.verify(dbWriter);
	}

	@Test
	public void testFinalizeImport() throws EPFDbException {
		EasyMock.reset(dbWriter);
		dbWriter.finalizeImport();
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(dbWriter);
		importTask.finalizeImport();
		EasyMock.verify(dbWriter);
	}
}
