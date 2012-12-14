package com.spazztv.epf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EPFImportTranslatorTest {

	EPFImportTranslator importTranslator;
	EPFFileReader fileReader;
	String recordSeparator = EPFConfig.EPF_RECORD_SEPARATOR_DEFAULT;
	String fieldSeparator = EPFConfig.EPF_FIELD_SEPARATOR_DEFAULT;
	String genreEpfFile = "testdata/epf_files/genre";
	String tvEpisodeGuide = "testdata/epf_flat_files/tvEpisode-usa.txt";

	@Before
	public void setUp() throws Exception {
		fileReader = new EPFFileReader(genreEpfFile, fieldSeparator, recordSeparator);
		importTranslator = new EPFImportTranslator(fileReader);
	}
	
	@Test
	public void testGetTotalRecordsExpected() {
		long recordsExpected = importTranslator.getTotalExpectedRecords();
		Assert.assertTrue(String.format("Invalid records expected. Expecting %d, received %s",1299L,recordsExpected), recordsExpected == 1299L);
	}

	@Test
	public void testGetLastRecordRead() {
		String[] row = null;
		for (int i = 0; i < 10; i++) {
			row = importTranslator.nextRecord();
			Assert.assertTrue("Invalid record returned", row != null);
		}
		long recordsExpected = 10L;
		long lastRecord = importTranslator.getLastRecordRead();
		Assert.assertTrue(String.format("Invalid last record read. Expecting %d, found %d",recordsExpected,lastRecord),lastRecord == recordsExpected);
	}
	
	@Test
	public void testGetTableName() throws Exception {
		EPFFileReader localReader = new EPFFileReader(genreEpfFile, fieldSeparator, recordSeparator);
		EPFImportTranslator localXlator = new EPFImportTranslator(localReader);
		String expectedTableName = "genre";
		String foundTableName = localXlator.getTableName();
		Assert.assertTrue(String.format("Invalid table name, expecting %s, found %s",expectedTableName,foundTableName),expectedTableName.equals(foundTableName));
		
		localReader = new EPFFileReader(tvEpisodeGuide, fieldSeparator, recordSeparator);
		localXlator = new EPFImportTranslator(localReader);
		expectedTableName = "tv_episode_usa";
		foundTableName = localXlator.getTableName();
		Assert.assertTrue(String.format("Invalid table name, expecting %s, found %s",expectedTableName,foundTableName),expectedTableName.equals(foundTableName));
	}

	@Test
	public void testGetColumnAndTypes() {
		Map<String,String> columnsAndTypes = importTranslator.getColumnAndTypes();
		Assert.assertTrue("Invalid value from getColumnsAndTypes()",columnsAndTypes != null);
		Entry<String,String> firstColumn = columnsAndTypes.entrySet().iterator().next();
		String expectedType = "BIGINT";
		String foundType = firstColumn.getValue();
		Assert.assertTrue(String.format("Invalid column type on first column, expecting %s, found %s",expectedType,foundType), expectedType.equals(foundType));
	}

	@Test
	public void testGetPrimaryKey() {
		List<String>expectedPrimaryKey = new ArrayList<String>();

		expectedPrimaryKey.add("genre_id");
		List<String>foundPrimaryKey = importTranslator.getPrimaryKey();
		Assert.assertTrue(String.format("Invalid number of primary key fields, expecting %d, found %d",expectedPrimaryKey.size(),foundPrimaryKey.size()), expectedPrimaryKey.size() == foundPrimaryKey.size());
		
		String expectedColumn = expectedPrimaryKey.iterator().next();
		String foundColumn = foundPrimaryKey.iterator().next();
		Assert.assertTrue(String.format("Invalid getPrimaryKey() column, expecting '%s', found '%s'",expectedColumn,foundColumn), expectedColumn.equals(foundColumn));
	}

	@Test
	public void testGetExportType() {
		EPFExportType foundExportType = importTranslator.getExportType();
		EPFExportType expectedExportType = EPFExportType.FULL;
		Assert.assertTrue(String.format("Invalid getExportType() - expecting %s, found %s",expectedExportType.toString(),foundExportType.toString()),expectedExportType == foundExportType);
	}

	@Test
	public void testNextDataRecord() {
		//Testing the first 10 records to make sure they match the data patterns
		long recordsExported = importTranslator.getTotalExpectedRecords();
		long foundRecords = 0;
		while (true) {
			String[] row = importTranslator.nextRecord();
			if (row == null) {
				break;
			}
			foundRecords++;
			//Genre test data fields
			//{export_date=BIGINT, genre_id=INTEGER, parent_id=INTEGER, name=VARCHAR(200)}
			String exportDate = row[0];
			String genreId = row[1];
			String parentId = row[2]; 
			String genreName = row[3];
			Assert.assertTrue(String.format("Invalid data returned for exportDate, expecting \\d+, found %s",exportDate),exportDate.matches("\\d+"));
			Assert.assertTrue(String.format("Invalid data returned for genreId, expecting \\d+, found %s",genreId),genreId.matches("\\d+"));
			if (parentId.length() > 0) {
				if (!parentId.matches("\\d+")) {
					System.out.println("There's a foul stench in the state of Denmark!");
				}
			
 				Assert.assertTrue(String.format("Invalid data returned for parentId, expecting \\d+, found %s",parentId),parentId.matches("\\d+"));
			}
			Assert.assertTrue(String.format("Invalid data returned for genreName, expecting .+, found: '%s'",genreName),genreName.matches(".+"));
		}
		Assert.assertTrue(String.format("Invalid number of records returned. Expected %d, found %d",recordsExported,foundRecords),recordsExported == foundRecords);
	}
}
