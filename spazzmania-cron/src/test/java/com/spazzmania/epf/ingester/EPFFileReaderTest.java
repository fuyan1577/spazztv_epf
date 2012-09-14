package com.spazzmania.epf.ingester;

import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class EPFFileReaderTest {

	EPFFileReader fileReader;
	String genreEpfFile = "testdata/epf_files/genre";
	String storefrontEpfFile = "testdata/epf_files/storefront";

	@Before
	public void setUp() throws Exception {
		fileReader = new EPFFileReader(genreEpfFile);
	}

	@Test
	public void testEPFFileReader() throws FileNotFoundException {
		EPFFileReader fileReader = new EPFFileReader(genreEpfFile);
		Assert.assertTrue("getFilePath returned the wrong value",
				genreEpfFile.equals(fileReader.getFilePath()));
	}

	@Test
	public void testGetRecordsExported() throws IOException {
		long foundRecordsExported = fileReader.getRecordsWritten();
		long expectedRecordsExported = 1299L;
		Assert.assertTrue(String.format("Invalid recordsExported expected %d, found %d",expectedRecordsExported, foundRecordsExported),
				expectedRecordsExported == foundRecordsExported);
	}

	@Test
	public void testRewind() throws IOException {
		fileReader.rewind();
		String nextHeaderLine = fileReader.nextHeaderLine();
		Assert.assertTrue("Invalid record after rewind", nextHeaderLine != null);
		Assert.assertTrue("Invalid record after rewind",
				nextHeaderLine.startsWith("#export_date"));
	}

	@Test
	public void testNextHeaderLine() throws IOException {
		fileReader.rewind();
		String nextHeaderLine = fileReader.nextHeaderLine();
		Assert.assertTrue("Invalid record after rewind", nextHeaderLine != null);
		Assert.assertTrue("Invalid record after rewind",
				nextHeaderLine.startsWith("#export_date"));
	}

	@Test
	public void testNextDataLine() throws IOException {
		fileReader.rewind();

		long recordsWritten = fileReader.getRecordsWritten();
		
		fileReader.rewind();
		int t = 0;
		while (fileReader.hasNextDataRecord()) {
			String nextDataLine = fileReader.nextDataLine();
			if (nextDataLine != null) {
				t++;
			}
			Assert.assertTrue("Invalid data record, expecting data, found NULL",
					nextDataLine != null);
			Assert.assertTrue("Invalid data record, expecting data matching %^\\d+\\x01.+$",
					nextDataLine.matches("^\\d+\\x01.+$"));
		}
		Assert.assertTrue(String.format("Incorrect total records. Expected %d, Found %d",recordsWritten,t),(t == recordsWritten));
	}
}
