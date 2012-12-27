package com.spazztv.epf.adapter;

import java.io.File;
import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.spazztv.epf.EPFConfig;
import com.spazztv.epf.EPFFileFormatException;
import com.spazztv.epf.EPFImportTranslator;
import com.spazztv.epf.adapter.EPFSpazzGameAppsFilter;
import com.spazztv.epf.adapter.SimpleEPFFileReader;

/**
 * This a JUnit test which actually loads small test files to verify the
 * loadApplicationIds() and testIsIncludeApplicationId methods.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFSpazzGameAppsFilterTest {

	private EPFSpazzGameAppsFilter appsFilter;
	private File directoryAndFile;
	private File directoryOnly;

	private static String EPF_DATA_GENRE_APPLICATION = "testdata/epf_files/genre_application";
	private static String EPF_DATA_DIRECTORY = "testdata/epf_files";

	@Before
	public void setUp() throws Exception {
		directoryAndFile = new File(EPF_DATA_GENRE_APPLICATION);
		directoryOnly = new File(EPF_DATA_DIRECTORY);
	}

	@Test
	public void testGetInstance() {
		// Test
		appsFilter = EPFSpazzGameAppsFilter.getInstance(directoryAndFile);

		File actualDirectory = appsFilter.getEpfDirectory();

		Assert.assertEquals(String.format(
				"Invalid diretory expecting %s, actual %s",
				directoryOnly.getName(), actualDirectory.getName()),
				directoryOnly, actualDirectory);
	}

	@Test
	public void testGetInstance2() {
		// Test
		appsFilter = EPFSpazzGameAppsFilter.getInstance(directoryOnly);

		File actualDirectory = appsFilter.getEpfDirectory();

		Assert.assertEquals(String.format(
				"Invalid diretory expecting %s, actual %s",
				directoryOnly.getName(), actualDirectory.getName()),
				directoryOnly, actualDirectory);
	}

	@Test
	public void testIsIncludeApplicationId() throws EPFFileFormatException,
			IOException {
		String expectedAppFile = directoryOnly.getPath() + File.separatorChar
				+ EPFSpazzGameAppsFilter.APPLICATION_FILE;

		EPFImportTranslator appReader = new EPFImportTranslator(
				new SimpleEPFFileReader(expectedAppFile,
						EPFConfig.EPF_FIELD_SEPARATOR_DEFAULT,
						EPFConfig.EPF_RECORD_SEPARATOR_DEFAULT));

		int actualGames = 0;
		int expectedGames = 5;

		appsFilter = EPFSpazzGameAppsFilter.getInstance(directoryOnly);

		while (appReader.hasNextRecord()) {
			List<String> record = appReader.nextRecord();
			String applicationId = record.get(1);
			if (appsFilter.isIncludeApplicationId(applicationId)) {
				actualGames++;
			}
		}

		Assert.assertTrue(String.format(
				"Incorrect number of games loaded: expected %d, actual %d",
				expectedGames, actualGames), (actualGames == expectedGames));
	}

}
