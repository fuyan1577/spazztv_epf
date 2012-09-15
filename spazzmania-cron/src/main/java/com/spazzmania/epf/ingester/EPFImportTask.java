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

	EPFImportXlator importXlator;
	EPFDbWriter dbWriter;
	long recordCount = 0;

	public EPFImportTask(EPFImportXlator importXlator, EPFDbWriter dbWriter) {
		this.importXlator = importXlator;
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
		dbWriter.initImport(importXlator.getExportType(),
				importXlator.getTableName(), importXlator.getColumnAndTypes(),
				importXlator.getTotalDataRecords());
	}

	public void importData() throws EPFImporterException {
		while (importXlator.hasNextRecord()) {
			recordCount++;
			dbWriter.insertRow(importXlator.nextRecord());
		}
		if (recordCount != importXlator.getTotalDataRecords()) {
			throw new EPFImporterException(
					String.format(
							"Incorrect number of import records. Expecting %d, found %d",
							importXlator.getTotalDataRecords(), recordCount));
		}
	}

	public void finalizeImport() throws EPFImporterException {
		dbWriter.finalizeImport();
	}
}
