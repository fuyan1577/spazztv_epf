package com.spazztv.epf;

import java.io.File;
import java.io.IOException;
import java.util.List;

import junit.framework.Assert;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

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

	private String businessGenre = "6000";
	private String gamesGenre = "6014";

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
	public void testLoadApplicationIds() throws Exception {
		EPFImportTranslator mockImportTranslator = EasyMock
				.createMock(EPFImportTranslator.class);
		SimpleEPFFileReader mockFileReader = EasyMock
				.createMock(SimpleEPFFileReader.class);
		File mockGenreApplicationFile = EasyMock.createMock(File.class);

		String expectedGenreAppFile = directoryOnly.getPath()
				+ File.separatorChar
				+ EPFSpazzGameAppsFilter.GENRE_APPLICATION_FILE;

		// These mocks are having no effect - the objects are being
		// instantiated.
		PowerMock.expectNew(File.class, expectedGenreAppFile).andReturn(
				mockGenreApplicationFile);
		PowerMock.replay(File.class);

		PowerMock.expectNew(SimpleEPFFileReader.class, EasyMock.anyObject(),
				EasyMock.anyObject(), EasyMock.anyObject()).andReturn(
				mockFileReader);
		PowerMock.replay(SimpleEPFFileReader.class);

		PowerMock.expectNew(EPFImportTranslator.class, EasyMock.anyObject())
				.andReturn(mockImportTranslator);
		PowerMock.replay(EPFImportTranslator.class);

		EasyMock.reset(mockGenreApplicationFile);
		EasyMock.replay(mockGenreApplicationFile);

		EasyMock.reset(mockFileReader);
		mockFileReader.getRecordsWritten();
		EasyMock.expectLastCall().andReturn(4L).times(1);
		EasyMock.replay(mockFileReader);

		EasyMock.reset(mockImportTranslator);
		mockImportTranslator.hasNextRecord();
		EasyMock.expectLastCall().andReturn(true).times(4);
		EasyMock.expectLastCall().andReturn(false).times(1);
		mockImportTranslator.nextRecord();
		EasyMock.expectLastCall()
				.andReturn(getMockGenreAppRecord(gamesGenre, "11111111"))
				.times(1);
		EasyMock.expectLastCall()
				.andReturn(getMockGenreAppRecord(businessGenre, "22222222"))
				.times(1);
		EasyMock.expectLastCall()
				.andReturn(getMockGenreAppRecord(gamesGenre, "33333333"))
				.times(1);
		EasyMock.expectLastCall()
				.andReturn(getMockGenreAppRecord(businessGenre, "44444444"))
				.times(1);
		EasyMock.replay(mockImportTranslator);

		appsFilter = EPFSpazzGameAppsFilter.getInstance(directoryOnly);

		appsFilter.loadApplicationIds();

		// PowerMock.verify(SimpleEPFFileReader.class);
		// PowerMock.verify(EPFImportTranslator.class);
		// EasyMock.verify(mockImportTranslator);
	}

	private List<String> getMockGenreAppRecord(String genreId, String appId) {
		return null;
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
		appsFilter.loadApplicationIds();

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
