package com.spazztv.logger;

import java.io.File;

import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;

import com.spazztv.epf.EPFImporter;
import com.spazztv.epf.dao.EPFFileReader;

@Aspect("perthis(call(* com.spazztv.epf.dao.EPFFileReader..*(..)))")
public class EPFFileReaderLogger {
	public static long RECORD_LOG_COUNT = 5000;
	public static long TIMESTAMP_LOG_COUNT = 120000;

	private long lastLoggedTimestamp = 0;
	private long recordsRead = 0;

	private String tableName = null;

	@After("    execution(* com.spazztv.epf.adapter.SimpleEPFFileReader.nextDataRecord(..)) && this(fileReader)")
	public void afterNextDataRecord(EPFFileReader fileReader) {
		if (tableName == null) {
			tableName = new File(fileReader.getFilePath()).getName();
		}
		
		recordsRead++;
		
		long recordsProcessed = EPFImportTaskInfoBlock.getInstance()
				.getRecordsProcessed(tableName) + 1;

		EPFImportTaskInfoBlock.getInstance().setRecordsProcessed(tableName,
				recordsProcessed);

		if (((recordsRead > RECORD_LOG_COUNT) || (System.currentTimeMillis() - lastLoggedTimestamp) > TIMESTAMP_LOG_COUNT)) {
			Logger log = EPFImporter.getLogger();
			long totalExportedRecords = EPFImportTaskInfoBlock.getInstance()
					.getTotalExportedRecords(tableName);
			log.info("{} - {} of {} records processed", tableName,
					recordsProcessed, totalExportedRecords);
			recordsRead = 0;
			lastLoggedTimestamp = System.currentTimeMillis();
		}
	}
}
