package com.spazztv.logger;

/**
 * Simple object for holding runtime statistics of an EPFImportTask. This object
 * is used by AspectJ logger aspects.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFImportTaskInfo {

	private String tableName = null;
	private long totalExportedRecords = 0;
	private long recordsProcessed = 0;
	private long recordsImported = 0;

	public EPFImportTaskInfo() {
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public long getTotalExportedRecords() {
		return totalExportedRecords;
	}

	public void setTotalExportedRecords(long totalExportedRecords) {
		this.totalExportedRecords = totalExportedRecords;
	}

	public long getRecordsProcessed() {
		return recordsProcessed;
	}

	public void setRecordsProcessed(long recordsProcessed) {
		this.recordsProcessed = recordsProcessed;
	}

	public long getRecordsImported() {
		return recordsImported;
	}

	public void setRecordsImported(long recordsImported) {
		this.recordsImported = recordsImported;
	}
}