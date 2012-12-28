/**
 * 
 */
package com.spazztv.logger;

import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;

import com.spazztv.epf.EPFImporter;
import com.spazztv.epf.dao.EPFDbWriter;

/**
 * @author billintj
 * 
 */
@Aspect("perthis(call(* com.spazztv.epf.dao.EPFDbWriter..*(..)))")
public class EPFDbWriterLogger {

	public static long RECORD_LOG_COUNT = 5000;
	public static long TIMESTAMP_LOG_COUNT = 120000;

	private long lastLoggedRecord = 0;
	private long lastLoggedTimestamp = 0;
	private long recordsImported = 0;

	private String tableName;

	@After("    execution(* com.spazztv.epf.dao.EPFDbWriter.insertRow(..)) && this(dbWriter)")
	public void afterInsertRow(EPFDbWriter dbWriter) {
		if (tableName == null) {
			tableName = dbWriter.getTableName();
		}
		recordsImported = EPFImportTaskInfoBlock.getInstance()
				.getRecordsImported(dbWriter.getTableName()) + 1;
		EPFImportTaskInfoBlock.getInstance().setRecordsImported(
				dbWriter.getTableName(), recordsImported);
		if (((recordsImported - lastLoggedRecord) > RECORD_LOG_COUNT)
				|| ((System.currentTimeMillis() - lastLoggedTimestamp) > TIMESTAMP_LOG_COUNT)) {
			Logger log = EPFImporter.getLogger();
			log.info("{} - {} records imported",
					tableName,
					recordsImported);
			lastLoggedTimestamp = System.currentTimeMillis();
		}
	}
}
