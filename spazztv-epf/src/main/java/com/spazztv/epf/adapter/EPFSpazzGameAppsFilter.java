/**
 * 
 */
package com.spazztv.epf.adapter;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.spazztv.epf.EPFConfig;
import com.spazztv.epf.EPFFileFormatException;
import com.spazztv.epf.EPFImportTranslator;
import com.spazztv.epf.dao.EPFFileReader;

/**
 * @author Thomas Billingsley
 * 
 */
public class EPFSpazzGameAppsFilter {

	public static String GENRE_APPLICATION_FILE = "genre_application";
	public static String APPLICATION_FILE = "application";

	public static int GENRE_APP_GENRE_ID = 1;
	public static int GENRE_APP_APPLICATION_ID = 2;
	public static int GENRE_APP_IS_PRIMARY = 3;

	public static int APPLICATION_APPLICATION_ID = 1;
	public static int APPLICATION_TITLE = 2;
	public static String CHEATS_STR = "cheats";

	public static String GAMES_GENRE_ID = "6014";
	public static String IS_PRIMARY = "1";

	private static EPFSpazzGameAppsFilter instance;
	private File epfDirectory;
	private String fieldSeparator;
	private String recordSeparator;
	
	private boolean applicationIdsLoaded = false;

	private ConcurrentHashMap<String, Boolean> applicationIds;

	/**
	 * EPFSpazzGameAppsFilter - this filter loads a hash map of Application IDs
	 * from the EPF Relational genre_application and application files.
	 * <p>
	 * The genre_application file is read for application's with a genre_id of
	 * 6014 (games) and is_primary = 1.
	 * <p>
	 * The application file is read for applications with titles containing the
	 * string 'cheats'.
	 * 
	 * @param epfDirectory
	 *            - of the EFP Relational files from iTunesCCYYMMDD
	 */
	private EPFSpazzGameAppsFilter(File epfDirectory) {
		if (epfDirectory.isDirectory()) {
			this.epfDirectory = epfDirectory;
		} else {
			this.epfDirectory = epfDirectory.getParentFile();
		}
		this.fieldSeparator = EPFConfig.EPF_FIELD_SEPARATOR_DEFAULT;
		this.recordSeparator = EPFConfig.EPF_RECORD_SEPARATOR_DEFAULT;
		this.applicationIds = new ConcurrentHashMap<String, Boolean>();
	}

	public static EPFSpazzGameAppsFilter getInstance(File epfDirectory) {
		if (epfDirectory == null) {
			return instance;
		}
		if ((instance == null) || !epfDirectory.equals(instance.epfDirectory)) {
			instance = new EPFSpazzGameAppsFilter(epfDirectory);
		}
		return instance;
	}

	public void loadApplicationIds() {
		try {
			loadGamesFromGenreApplication();
			loadCheatsFromApplication();
		} catch (EPFFileFormatException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		applicationIdsLoaded = true;
	}

	private void loadGamesFromGenreApplication() throws EPFFileFormatException,
			IOException {

		File genreApplicationFile = new File(epfDirectory.getPath()
				+ File.separatorChar + GENRE_APPLICATION_FILE);
		EPFFileReader fileReader = new SimpleEPFFileReader(genreApplicationFile.getPath(),
				fieldSeparator, recordSeparator); 
		EPFImportTranslator genreApplication = new EPFImportTranslator(fileReader);
	
		List<String> genreAppRecord;
		while (genreApplication.hasNextRecord()) {
			genreAppRecord = genreApplication.nextRecord();
			if (genreAppRecord.get(GENRE_APP_GENRE_ID).equals(GAMES_GENRE_ID)
					&& genreAppRecord.get(GENRE_APP_IS_PRIMARY).equals(
							IS_PRIMARY)) {
				applicationIds.put(
						genreAppRecord.get(GENRE_APP_APPLICATION_ID), true);
			}
		}
	}

	private void loadCheatsFromApplication() throws EPFFileFormatException,
			IOException {
		File applicationFile = new File(epfDirectory.getPath()
				+ File.separatorChar + APPLICATION_FILE);

		EPFImportTranslator appReader = new EPFImportTranslator(
				new SimpleEPFFileReader(applicationFile.getPath(),
						fieldSeparator, recordSeparator));

		List<String> appRecord;
		while (appReader.hasNextRecord()) {
			appRecord = appReader.nextRecord();
			if (appRecord.get(APPLICATION_TITLE).contains(CHEATS_STR)) {
				applicationIds.put(appRecord.get(APPLICATION_APPLICATION_ID),
						true);
			}
		}
	}

	public synchronized boolean isIncludeApplicationId(String applicationId) {
		if (applicationId == null) {
			return false;
		}
		if (!applicationIdsLoaded) {
			loadApplicationIds();
		}
			
		return applicationIds.containsKey(applicationId);
	}

	public File getEpfDirectory() {
		return epfDirectory;
	}
}
