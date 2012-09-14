package com.spazzmania.epf.ingester;

import static org.junit.Assert.fail;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class EPFImportXlatorTest {

	EPFImportXlator importXlator;
	EPFFileReader fileReader;
	String genreEpfFile = "testdata/epf_files/genre";
	String storefrontEpfFile = "testdata/epf_files/storefront";

	@Before
	public void setUp() throws Exception {
		fileReader = new EPFFileReader(genreEpfFile);
		importXlator = new EPFImportXlator(fileReader,
				EPFImportXlator.DEFAULT_ROW_SEPARATOR, EPFImportXlator.DEFAULT_FIELD_SEPARATOR);
	}

	@Test
	public void testGetTotalRecordsExpected() {
		long recordsExpected = importXlator.getTotalRecordsExpected();
		Assert.assertTrue(String.format("Invalid records expected. Expecting %d, received %s",1299L,recordsExpected), recordsExpected == 1299L);
	}

	@Test
	public void testGetLastRecordRead() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetColumnAndTypes() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetPrimaryKey() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetExportType() {
		fail("Not yet implemented");
	}

	@Test
	public void testNextDataRecord() {
		fail("Not yet implemented");
	}

}
