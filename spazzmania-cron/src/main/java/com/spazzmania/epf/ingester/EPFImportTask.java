/**
 * 
 */
package com.spazzmania.epf.ingester;

/**
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFImportTask implements Runnable {

	EPFImportTranslator importTranslator;
	EPFDbWriter dbWriter;
	long recordCount = 0;

	public EPFImportTask(EPFImportTranslator importTranslator, EPFDbWriter dbWriter) {
		this.importTranslator = importTranslator;
		this.dbWriter = dbWriter;
	}

	@Override
	public void run() {
		try {
			setupImportDataStore();
			importData();
			finalizeImport();
		} catch (EPFImporterException e) {
			e.printStackTrace();
		}
	}

	public void setupImportDataStore() throws EPFImporterException {
		dbWriter.initImport(importTranslator.getExportType(),
				importTranslator.getTableName(), importTranslator.getColumnAndTypes(),
				importTranslator.getTotalDataRecords());
	}

	public void importData() throws EPFImporterException {
		while (importTranslator.hasNextRecord()) {
			recordCount++;
			dbWriter.insertRow(importTranslator.nextRecord());
		}
		if (recordCount != importTranslator.getTotalDataRecords()) {
			throw new EPFImporterException(
					String.format(
							"Incorrect number of import records. Expecting %d, found %d",
							importTranslator.getTotalDataRecords(), recordCount));
		}
	}

	public void finalizeImport() throws EPFImporterException {
		dbWriter.finalizeImport();
	}
}
