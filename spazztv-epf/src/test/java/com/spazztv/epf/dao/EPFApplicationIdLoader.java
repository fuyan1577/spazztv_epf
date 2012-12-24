/**
 * 
 */
package com.spazztv.epf.dao;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import com.spazztv.epf.EPFConfig;
import com.spazztv.epf.EPFFileFormatException;
import com.spazztv.epf.EPFFileReader;

/**
 * @author Thomas Billingsley
 *
 */
public class EPFApplicationIdLoader {
	
	private EPFFileReader fileReader;
	private File epfDirectory;
	private List<String> applicationIds = null;
	
	public EPFApplicationIdLoader(File epfDirectory) {
		this.epfDirectory = epfDirectory;
		applicationIds = new ArrayList<String>();
	}
	
	public void loadGamesApplicationIds() throws IOException, EPFFileFormatException {
		String primaryGenreId = "6014";
		String genreApplicationPath = epfDirectory.getPath() + "/" + "genre_application";
		fileReader = new EPFFileReader(genreApplicationPath, EPFConfig.EPF_FIELD_SEPARATOR_DEFAULT, EPFConfig.EPF_RECORD_SEPARATOR_DEFAULT);
		while (fileReader.hasNextDataRecord()) {
			List<String> record = fileReader.nextDataRecord();
			if (record.get(1).equals(primaryGenreId) && record.get(3).equals("1")) {
				applicationIds.add(record.get(2));
			}
		}
	}

	public void loadCheatsApplicationIds() throws IOException, EPFFileFormatException {
		String applicationPath = epfDirectory.getPath() + "/" + "application";
		fileReader = new EPFFileReader(applicationPath, EPFConfig.EPF_FIELD_SEPARATOR_DEFAULT, EPFConfig.EPF_RECORD_SEPARATOR_DEFAULT);
		Pattern cheatsPat = Pattern.compile(".*cheats.*", Pattern.CASE_INSENSITIVE);
		while (fileReader.hasNextDataRecord()) {
			List<String> record = fileReader.nextDataRecord();
			String title = record.get(2);
			if (cheatsPat.matcher(title).matches()) {
				applicationIds.add(record.get(1));
			}
		}
	}
	
	public List<String> getApplicationIds() {
		return applicationIds;
	}
	
	public static void main(String[] args) throws IOException, EPFFileFormatException {
		long startTime = new Date().getTime();
		if (args.length < 1) {
			System.out.println("Usage: EPFApplicationIdLoader [FILEPATH/]genre_application");
			System.exit(1);
		}
		EPFApplicationIdLoader loader = new EPFApplicationIdLoader(new File(args[0]));
		loader.loadGamesApplicationIds();
		loader.loadCheatsApplicationIds();
		List<String> appIds = loader.getApplicationIds();
		long endTime = new Date().getTime();
		long elapsedSeconds = (endTime - startTime) / 1000;
		System.out.println(String.format("Loaded %d Games/Cheats Application Ids, %d seconds",appIds.size(),elapsedSeconds));
	}
}
