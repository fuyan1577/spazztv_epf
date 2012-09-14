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
	public void testReadRecordsWrittenLine() throws IOException {
		String recordsWrittenLine = fileReader.readRecordsWrittenLine();
		Assert.assertTrue("Invalid recordsWritten line", recordsWrittenLine != null);
		Assert.assertTrue("Invalid recordsWritten line", recordsWrittenLine.matches(".+recordsWritten:\\d+.+"));
	}

	@Test
	public void testRewind() throws IOException {
		fileReader.rewind();
		String nextHeaderLine = fileReader.readNextHeaderLine();
		Assert.assertTrue("Invalid record after rewind", nextHeaderLine != null);
		Assert.assertTrue("Invalid record after rewind", nextHeaderLine.startsWith("#export_date"));
	}

	@Test
	public void testReadNextHeaderLine() throws IOException {
		fileReader.rewind();
		String nextHeaderLine = fileReader.readNextHeaderLine();
		Assert.assertTrue("Invalid record after rewind", nextHeaderLine != null);
		Assert.assertTrue("Invalid record after rewind", nextHeaderLine.startsWith("#export_date"));
	}

	@Test
	public void testReadNextDataLine() throws IOException {
		fileReader.rewind();
		String nextDataLine = fileReader.readNextDataLine();
		
	}
}
