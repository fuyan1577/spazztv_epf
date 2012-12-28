package com.spazztv.logger;

import java.util.concurrent.ConcurrentHashMap;

public class EPFImportTaskInfoBlock {

	private static EPFImportTaskInfoBlock instance;

	private ConcurrentHashMap<String, EPFImportTaskInfo> taskHash;

	private EPFImportTaskInfoBlock() {
		taskHash = new ConcurrentHashMap<String, EPFImportTaskInfo>();
	}

	public static EPFImportTaskInfoBlock getInstance() {
		if (instance == null) {
			instance = new EPFImportTaskInfoBlock();
		}
		return instance;
	}

	public synchronized long getTotalExportedRecords(String tableName) {
		checkTable(tableName);
		return taskHash.get(tableName).getTotalExportedRecords();
	}

	public synchronized void setTotalExportedRecords(String tableName,
			long totalExportedRecords) {
		checkTable(tableName);
		taskHash.get(tableName).setTotalExportedRecords(totalExportedRecords);
	}

	public synchronized long getRecordsProcessed(String tableName) {
		checkTable(tableName);
		return taskHash.get(tableName).getRecordsProcessed();
	}

	public synchronized void setRecordsProcessed(String tableName,
			long recordsProcessed) {
		checkTable(tableName);
		taskHash.get(tableName).setRecordsProcessed(recordsProcessed);
	}

	public synchronized long getRecordsImported(String tableName) {
		checkTable(tableName);
		return taskHash.get(tableName).getRecordsImported();
	}

	public synchronized void setRecordsImported(String tableName,
			long recordsImported) {
		checkTable(tableName);
		taskHash.get(tableName).setRecordsImported(recordsImported);
	}
	
	private void checkTable(String tableName) {
		if (!taskHash.containsKey(tableName)) {
			taskHash.put(tableName, new EPFImportTaskInfo());
		}
	}
	
	public void clear() {
		taskHash.clear();
	}
}
