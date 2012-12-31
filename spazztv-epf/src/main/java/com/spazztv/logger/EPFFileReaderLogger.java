package com.spazztv.logger;

import java.io.File;
import java.util.List;

import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;

import com.spazztv.epf.EPFImporter;
import com.spazztv.epf.dao.EPFFileReader;

@Aspect("perthis(call(* com.spazztv.epf.adapter.SimpleEPFFileReader..*(..)))")
public class EPFFileReaderLogger {
	public static long RECORD_LOG_COUNT = 5000;
	public static long TIMESTAMP_LOG_COUNT = 120000;

	private long lastLoggedTimestamp = 0;
	private String tableName = null;

	/**
	 * Log the progress of data records read by the EPFFileREader object.
	 * <p>
	 * Wait at least 2 minutes between log messages and display counts which are
	 * multiples of 5000 records.
	 * 
	 * @param dbWriter
	 */
	@AfterReturning(pointcut = "execution(* com.spazztv.epf.adapter.SimpleEPFFileReader.nextDataRecord(..)) && this(fileReader)", returning = "record")
	public void afterNextDataRecord(EPFFileReader fileReader,
			List<String> record) {
		// Skip the null record returned at end of file
		if (record == null) {
			return;
		}

		if (tableName == null) {
			tableName = new File(fileReader.getFilePath()).getName();
		}

		long recordsProcessed = EPFImportTaskInfoBlock.getInstance()
				.getRecordsProcessed(tableName) + 1;

		EPFImportTaskInfoBlock.getInstance().setRecordsProcessed(tableName,
				recordsProcessed);

		if ((System.currentTimeMillis() - lastLoggedTimestamp) > TIMESTAMP_LOG_COUNT
				&& ((recordsProcessed % RECORD_LOG_COUNT) == 0)) {
			Logger log = EPFImporter.getLogger();
			long totalExportedRecords = fileReader.getRecordsWritten();
			log.info("{} - {} of {} records processed", tableName,
					recordsProcessed, totalExportedRecords);
			lastLoggedTimestamp = System.currentTimeMillis();
		}
	}

	@After("execution(void com.spazztv.epf.adapter.SimpleEPFFileReader.close(..)) && this(fileReader)")
	public void afterClose(EPFFileReader fileReader) {
		Logger log = EPFImporter.getLogger();

		long totalExportedRecords = fileReader.getRecordsWritten();
		long recordsProcessed = EPFImportTaskInfoBlock.getInstance()
				.getRecordsProcessed(tableName);
		log.info("closing {} - {} of {} records processed", tableName,
				recordsProcessed, totalExportedRecords);
	}
}
