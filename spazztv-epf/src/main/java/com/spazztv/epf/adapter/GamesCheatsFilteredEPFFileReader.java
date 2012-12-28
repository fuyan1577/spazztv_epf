/**
 * 
 */
package com.spazztv.epf.adapter;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import com.spazztv.epf.EPFFileFormatException;
import com.spazztv.epf.dao.EPFFileReader;

/**
 * @author Thomas Billingsley
 * 
 */
public class GamesCheatsFilteredEPFFileReader extends EPFFileReader {

	public static String[] APP_ID_FILES = { "application",
			"application_detail", "application_device_type",
			"genre_application", "application_popularity_per_genre",
			"application_price", "artist_application" };

	public static Integer[] APP_ID_OFFSETS = { 1, 1, 1, 2, 3, 1, 2 };

	private EPFFileReader fileReader;
	private EPFSpazzGameAppsFilter appsFilter;
	private HashMap<String, Integer> appIdColumns;
	private boolean requiresFiltering = false;
	private int appIdColumn = -1;
	private List<String> nextDataRecord;

	public GamesCheatsFilteredEPFFileReader(String filePath, String fieldSeparator,
			String recordSeparator) throws IOException, EPFFileFormatException {
		super(filePath, fieldSeparator, recordSeparator);
		
		initAppIdColumns();

		fileReader = new SimpleEPFFileReader(filePath, fieldSeparator,
				recordSeparator);

		appsFilter = EPFSpazzGameAppsFilter.getInstance(new File(filePath));
		
		checkIfFileRequiresFiltering(filePath);
	}
	
	private void checkIfFileRequiresFiltering(String filePath) {
		String fileName = new File(filePath).getName();
		requiresFiltering = appIdColumns.containsKey(fileName);
		if (requiresFiltering) {
			appIdColumn = appIdColumns.get(fileName);
		}
	}

	public boolean isRequiresFiltering() {
		return requiresFiltering;
	}

	private void initAppIdColumns() {
		appIdColumns = new HashMap<String, Integer>();
		for (int i = 0; i < APP_ID_FILES.length; i++) {
			appIdColumns.put(APP_ID_FILES[i], APP_ID_OFFSETS[i]);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.spazztv.epf.EPFFileReader#getRecordsWritten()
	 */
	@Override
	public long getRecordsWritten() {
		return fileReader.getRecordsWritten();
	}

	/**
	 * Retrieve next data record per the application id filter.
	 * <p>
	 * For import files with aplication_id fields, only those which pass through
	 * the EPFSpazzGameAppsFilter are included.
	 * <p>
	 * For all other import files, no filters are applied.
	 * 
	 * @see com.spazztv.epf.dao.EPFFileReader#nextDataRecord()
	 */
	@Override
	public List<String> nextDataRecord() {
		if (nextDataRecord == null) {
			if (!hasNextDataRecord()) {
				return null;
			}
		}
		List<String> curRecord = nextDataRecord;
		nextDataRecord = null;
		return curRecord;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.spazztv.epf.EPFFileReader#nextHeaderRecord()
	 */
	@Override
	public List<String> nextHeaderRecord() {
		return fileReader.nextHeaderRecord();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.spazztv.epf.EPFFileReader#rewind()
	 */
	@Override
	public void rewind() throws IOException {
		fileReader.rewind();
		nextDataRecord = null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.spazztv.epf.EPFFileReader#hasNextDataRecord()
	 */
	@Override
	public boolean hasNextDataRecord() {
		boolean done = false;
		while (!done) {
			done = true;
			if (fileReader.hasNextDataRecord()) {
				nextDataRecord = fileReader.nextDataRecord();
				if (isRequiresFiltering() && nextDataRecord != null) {
					if (!appsFilter.isIncludeApplicationId(nextDataRecord
							.get(appIdColumn))) {
						done = false;
						nextDataRecord = null;
					}
				}
			}
		}
		return (nextDataRecord != null);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.spazztv.epf.EPFFileReader#close()
	 */
	@Override
	public void close() throws IOException {
		fileReader.close();
	}

}
