package com.spazztv.epf;

import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class EPFFileReaderTest {

	EPFFileReader fileReader;
	String genreEpfFile = "testdata/epf_files/genre";
	String epfEPFInvalidFormatFile = "testdata/epf_files/invalid_epf_file";
	String recordSeparator = EPFConfig.EPF_RECORD_SEPARATOR_DEFAULT;
	String fieldSeparator = EPFConfig.EPF_FIELD_SEPARATOR_DEFAULT;

	@Before
	public void setUp() throws Exception {
		fileReader = new EPFFileReader(genreEpfFile, fieldSeparator,
				recordSeparator);
	}

	@Test
	public void testEPFFileFormatException() throws IOException {
		boolean exceptionThrown = false;
		try {
			@SuppressWarnings("unused")
			EPFFileReader localReader = new EPFFileReader(
					epfEPFInvalidFormatFile, fieldSeparator, recordSeparator);
			@SuppressWarnings("unused")
			EPFImportTranslator localXlator = new EPFImportTranslator(
					fileReader);
		} catch (EPFFileFormatException e) {
			exceptionThrown = true;
		}
		Assert.assertTrue(
				"Expecting an EPFFileFormatException, exception was not thrown on an invalid file",
				exceptionThrown == true);
	}

	@Test
	public void testEPFFileReader() throws IOException,
			EPFFileFormatException {
		EPFFileReader fileReader = new EPFFileReader(genreEpfFile,
				fieldSeparator, recordSeparator);
		Assert.assertTrue("getFilePath returned the wrong value",
				genreEpfFile.equals(fileReader.getFilePath()));
	}

	@Test
	public void testGetRecordsExported() throws IOException {
		long foundRecordsExported = fileReader.getRecordsWritten();
		long expectedRecordsExported = 1299L;
		Assert.assertTrue(String.format(
				"Invalid recordsExported expected %d, found %d",
				expectedRecordsExported, foundRecordsExported),
				expectedRecordsExported == foundRecordsExported);
	}

	@Test
	public void testRewind() throws IOException {
		fileReader.rewind();
		List<String> nextHeaderLine = fileReader.nextHeaderRecord();
		Assert.assertTrue("Invalid record after rewind", nextHeaderLine != null);
		Assert.assertTrue("Invalid record after rewind", nextHeaderLine.size() > 0);
		Assert.assertTrue("Invalid record after rewind",
				nextHeaderLine.get(0).startsWith("#export_date"));
	}

	@Test
	public void testNextHeaderLine() throws IOException {
		fileReader.rewind();
		List<String> nextHeaderLine = fileReader.nextHeaderRecord();
		Assert.assertTrue("Invalid record after rewind", nextHeaderLine != null);
		Assert.assertTrue("Invalid record after rewind", nextHeaderLine.size() > 0);
		Assert.assertTrue("Invalid record after rewind",
				nextHeaderLine.get(0).startsWith("#export_date"));
	}

	@Test
	public void testNextDataLine() throws IOException {
		fileReader.rewind();

		long recordsWritten = fileReader.getRecordsWritten();

		fileReader.rewind();
		int t = 0;
		while (fileReader.hasNextDataRecord()) {
			List<String> nextDataLine = fileReader.nextDataRecord();
			if (nextDataLine != null) {
				t++;
			}
			Assert.assertTrue(
					"Invalid data record, expecting data, found NULL",
					nextDataLine != null);
			Assert.assertTrue(
					"Invalid data record, expecting data matching %^\\d+\\x01.+$",
					nextDataLine.get(0).matches("^\\d+$"));
		}
		Assert.assertTrue(String.format(
				"Incorrect total records. Expected %d, Found %d",
				recordsWritten, t), (t == recordsWritten));
	}
}
