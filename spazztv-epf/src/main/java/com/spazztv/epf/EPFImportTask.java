/**
 * 
 */
package com.spazztv.epf;

import java.io.FileNotFoundException;

import com.spazztv.epf.dao.EPFDbException;
import com.spazztv.epf.dao.EPFDbWriter;

/**
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFImportTask implements Runnable {

	private EPFImportTranslator importTranslator;
	private EPFDbWriter dbWriter;
	private long recordCount = 0;
	private long lastLoggedCount = 0;
	private long lastLoggedTime;
	
	public static long RECORD_GAP = 5000;
	public long TIME_GAP = 120000; // milliseconds - 2 minutes

	public EPFImportTask(String filePath, EPFDbWriter dbWriter)
			throws FileNotFoundException, EPFFileFormatException {
		importTranslator = new EPFImportTranslator(new EPFFileReader(filePath));
		this.dbWriter = dbWriter;
	}

	@Override
	public void run() {
		try {
			setupImportDataStore();
			importData();
			finalizeImport();
		} catch (EPFDbException e) {
			//Logger getTableName() - Import Error may not have completed
			e.printStackTrace();
		}
	}

	public void setupImportDataStore() throws EPFDbException {
		dbWriter.initImport(importTranslator.getExportType(),
				importTranslator.getTableName(),
				importTranslator.getColumnAndTypes(),
				importTranslator.getPrimaryKey(),
				importTranslator.getTotalDataRecords());
	}

	public void importData() throws EPFDbException {
		try {
			while (importTranslator.hasNextRecord()) {
				recordCount++;
				dbWriter.insertRow(importTranslator.nextRecord());
				logProgress();
			}
			if (recordCount != importTranslator.getTotalDataRecords()) {
				String errMsg = String
						.format("Incorrect number of import records. Expecting %d, found %d",
								importTranslator.getTotalDataRecords(),
								recordCount);
				// Log dbConfig.getTableName() ${errMsg}
				throw new EPFDbException(errMsg);
			}
		} catch (EPFDbException e) {
			EPFImporterQueue.getInstance().setFailed(
					importTranslator.getFilePath());
			throw e;
		}
	}

	private void logProgress() {
		if ((recordCount - lastLoggedCount) > RECORD_GAP) {
			if ((System.currentTimeMillis() - lastLoggedTime) > TIME_GAP) {
				lastLoggedCount = recordCount;
				lastLoggedTime = System.currentTimeMillis();
				// Log getTableName(): at %d of %d records...
			}
		}
	}

	public void finalizeImport() throws EPFDbException {
		// Log dbConfig.getTableName() finalizing import
		dbWriter.finalizeImport();
		EPFImporterQueue.getInstance().setCompleted(
				importTranslator.getFilePath());
		// Log dbConfig.getTableName() import completed
	}
}
