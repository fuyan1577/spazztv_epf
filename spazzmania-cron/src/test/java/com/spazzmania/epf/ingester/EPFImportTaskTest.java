package com.spazzmania.epf.ingester;

import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.util.LinkedHashMap;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class EPFImportTaskTest {

	String storefrontEpfFiles = "testdata/epf_files/storefront";
	EPFImportTask importTask;
	EPFImportXlator importXlator;
	EPFDbWriter dbWriter;
	long recordsExpected = 0;
	
	EPFExportType exportType;
	String tableName;
	LinkedHashMap<String,String> columnsAndTypes;
	
//	dbWriter.initImport(importXlator.getExportType(),
//			importXlator.getTableName(), importXlator.getColumnAndTypes(),
//			importXlator.getTotalDataRecords());
	
	@Before
	public void setUp() throws FileNotFoundException, EPFFileFormatException {
		//Setting up importXlator to read the storefront data file
		//Setting up dbWriter as a mock object
		EPFFileReader fileReader = new EPFFileReader(storefrontEpfFiles);
		importXlator = new EPFImportXlator(fileReader);
		recordsExpected = importXlator.getTotalDataRecords();
		dbWriter = EasyMock.createMock(EPFDbWriter.class);
		
		importTask = new EPFImportTask(importXlator,dbWriter);
		
		exportType = importXlator.getExportType();
		tableName = importXlator.getTableName();
		columnsAndTypes = importXlator.getColumnAndTypes();
	}

	@Test
	public void testSetupImportDataStore() throws EPFImporterException {
		EasyMock.reset(dbWriter);
		dbWriter.initImport(exportType, tableName, columnsAndTypes, recordsExpected);
		EasyMock.expectLastCall().times(1);
		EasyMock.replay(dbWriter);
		importTask.setupImportDataStore();
		EasyMock.verify(dbWriter);
	}

	@Test
	public void testImportData() {
		fail("Not yet implemented");
	}

	@Test
	public void testFinalizeImport() {
		fail("Not yet implemented");
	}

}
