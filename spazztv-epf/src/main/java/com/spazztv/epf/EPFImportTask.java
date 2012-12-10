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
				importTranslator.getTotalExpectedRecords());
	}

	public void importData() throws EPFDbException {
		try {
			while (importTranslator.hasNextRecord()) {
				recordCount++;
				dbWriter.insertRow(importTranslator.nextRecord());
			}
			if (recordCount != importTranslator.getTotalExpectedRecords()) {
				String errMsg = String
						.format("Incorrect number of import records. Expecting %d, found %d",
								importTranslator.getTotalExpectedRecords(),
								recordCount);
				throw new EPFDbException(errMsg);
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
