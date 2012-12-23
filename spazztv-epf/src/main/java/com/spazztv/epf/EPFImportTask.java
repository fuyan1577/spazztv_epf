/**
 * 
 */
package com.spazztv.epf;

import java.io.IOException;

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
	
	public static long RECORD_GAP = 5000;
	public long TIME_GAP = 120000; // milliseconds - 2 minutes

	public EPFImportTask(String filePath, String fieldSeparator, String recordSeparator, EPFDbWriter dbWriter)
			throws IOException, EPFFileFormatException {
		importTranslator = new EPFImportTranslator(new EPFFileReader(filePath, fieldSeparator, recordSeparator));
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
				importTranslator.getTotalExpectedRecords());
	}

	public void importData() throws EPFDbException {
		try {
			while (importTranslator.hasNextRecord()) {
				dbWriter.insertRow(importTranslator.nextRecord());
			}
		} catch (EPFDbException e) {
			EPFImporterQueue.getInstance().setFailed(
					importTranslator.getFilePath());
			throw e;
		}
	}

	public void finalizeImport() throws EPFDbException {
		dbWriter.finalizeImport();
		EPFImporterQueue.getInstance().setCompleted(
				importTranslator.getFilePath());
	}
	
	/**
	 * EPFImportTranslator Getter for logger info
	 * @return EPFImportTranslator
	 */
	public EPFImportTranslator getImportTranslator() {
		return importTranslator;
	}

	/**
	 * EPFDbWriter Getter for logger info
	 * @return EPFDbWriter
	 */
	public EPFDbWriter getDbWriter() {
		return dbWriter;
	}
}
