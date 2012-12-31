package com.spazztv.epf.adapter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.spazztv.epf.EPFConfig;
import com.spazztv.epf.EPFFileFormatException;

public class GamesCheatsFilteredEPFFileReaderTest {

	private GamesCheatsFilteredEPFFileReader fileReader;

	private String applicationFile = "testdata/epf_files/application";
	private String genreFile = "testdata/epf_files/genre";
	private String genreApplicationFile = "testdata/epf_files/genre_application";
	private String applicationPriceFile = "testdata/epf_files/application_price";
	private String recordSeparator = EPFConfig.EPF_RECORD_SEPARATOR_DEFAULT;
	private String fieldSeparator = EPFConfig.EPF_FIELD_SEPARATOR_DEFAULT;
	private int expectedAppColumns = 17;
	private int expectedAppPriceColumns = 5;
	private int expectedGenreAppColumns = 4;
	private long expectedDataRecords = 5;
	private long expectedHeaderRecords = 5;

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testIsRequiresFiltering() throws IOException,
			EPFFileFormatException {
		GamesCheatsFilteredEPFFileReader nonFilteredReader = new GamesCheatsFilteredEPFFileReader(
				genreFile, fieldSeparator, recordSeparator);
		Assert.assertFalse(
				"Invalid filtering status, expected no filtering required for 'genre' file",
				nonFilteredReader.isRequiresFiltering());
		GamesCheatsFilteredEPFFileReader filteredReader = new GamesCheatsFilteredEPFFileReader(
				applicationFile, fieldSeparator, recordSeparator);
		Assert.assertTrue(
				"Invalid filtering status, expected filtering required for 'application' file",
				filteredReader.isRequiresFiltering());
	}

	@Test
	public void testNextDataRecordApp() throws IOException, EPFFileFormatException {
		fileReader = new GamesCheatsFilteredEPFFileReader(applicationFile,
				fieldSeparator, recordSeparator);
		
		long actualDataRecords = 0;
		while (fileReader.hasNextDataRecord()) {
			actualDataRecords++;

			List<String> dataRecord = fileReader.nextDataRecord();
			int actualColumns = dataRecord.size();
			Assert.assertTrue(String.format(
					"Invalid number of columns, expecting %d, actual %d",
					expectedAppColumns, actualColumns),
					expectedAppColumns == actualColumns);
		}

		Assert.assertTrue(String.format(
				"Invalid number of filtered records, expecting %d, actual %d",
				expectedDataRecords, actualDataRecords),
				expectedDataRecords == actualDataRecords);
	}

	@Test
	public void testNextDataRecordAppPrice() throws IOException, EPFFileFormatException {
		fileReader = new GamesCheatsFilteredEPFFileReader(applicationPriceFile,
				fieldSeparator, recordSeparator);
		
		long actualDataRecords = 0;
		while (fileReader.hasNextDataRecord()) {
			actualDataRecords++;

			List<String> dataRecord = fileReader.nextDataRecord();
			int actualColumns = dataRecord.size();
			Assert.assertTrue(String.format(
					"Invalid number of columns, expecting %d, actual %d",
					expectedAppPriceColumns, actualColumns),
					expectedAppPriceColumns == actualColumns);
		}

		Assert.assertTrue(String.format(
				"Invalid number of filtered records, expecting %d, actual %d",
				expectedDataRecords, actualDataRecords),
				expectedDataRecords == actualDataRecords);
	}
	
	@Test
	public void testNextDataRecordGenreApp() throws IOException, EPFFileFormatException {
		fileReader = new GamesCheatsFilteredEPFFileReader(genreApplicationFile,
				fieldSeparator, recordSeparator);
		
		long actualDataRecords = 0;
		while (fileReader.hasNextDataRecord()) {
			actualDataRecords++;

			List<String> dataRecord = fileReader.nextDataRecord();
			int actualColumns = dataRecord.size();
			Assert.assertTrue(String.format(
					"Invalid number of columns, expecting %d, actual %d",
					expectedGenreAppColumns, actualColumns),
					expectedGenreAppColumns == actualColumns);
		}

		Assert.assertTrue(String.format(
				"Invalid number of filtered records, expecting %d, actual %d",
				expectedDataRecords, actualDataRecords),
				expectedDataRecords == actualDataRecords);
	}

	@Test
	public void testNextHeaderRecord() throws IOException, EPFFileFormatException {
		fileReader = new GamesCheatsFilteredEPFFileReader(applicationFile,
				fieldSeparator, recordSeparator);
		
		HashMap<String, Boolean> verifiedHeaderTypes = new HashMap<String, Boolean>();
		verifiedHeaderTypes.put("#export_date", false);
		verifiedHeaderTypes.put("#primaryKey", false);
		verifiedHeaderTypes.put("#dbTypes", false);
		verifiedHeaderTypes.put("#exportMode", false);
		verifiedHeaderTypes.put("##legal", false);
		long actualHeaderRecords = 0;

		List<String> headerRecord;
		while ((headerRecord = fileReader.nextHeaderRecord()) != null) {
			actualHeaderRecords++;

			String typeTag = headerRecord.get(0).split("[\\:\\x01]")[0];

			Assert.assertTrue(String.format("Invalid header type %s", typeTag),
					verifiedHeaderTypes.containsKey(typeTag));

			verifiedHeaderTypes.put(typeTag, true);

			if (typeTag.equals("##legal")) {
				break;
			}
		}

		for (String key : verifiedHeaderTypes.keySet()) {
			Assert.assertTrue(String.format("Missing header key %s", key),
					verifiedHeaderTypes.get(key));
		}

		Assert.assertTrue(String.format(
				"Invalid number of header records, expecting %d, actual %d",
				expectedHeaderRecords, actualHeaderRecords),
				expectedHeaderRecords == actualHeaderRecords);
	}

	@Test
	public void testRewind() throws IOException, EPFFileFormatException {
		fileReader = new GamesCheatsFilteredEPFFileReader(applicationFile,
				fieldSeparator, recordSeparator);
		
		List<String> firstDataRecord = null;
		while (fileReader.hasNextDataRecord()) {
			List<String> dataRecord = fileReader.nextDataRecord();
			if (firstDataRecord == null) {
				firstDataRecord = dataRecord;
			}
		}

		fileReader.rewind();

		List<String> actualRecord = fileReader.nextDataRecord();

		Assert.assertEquals(
				"Rewind failed, first data record after rewind does not match",
				firstDataRecord, actualRecord);
	}
}
