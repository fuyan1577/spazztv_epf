/**
 * 
 */
package com.spazztv.logger;

import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;

import com.spazztv.epf.EPFImportTranslator;
import com.spazztv.epf.EPFImporter;
import com.spazztv.epf.dao.EPFFileReader;

/**
 * @author Thomas Billingsley
 * 
 */
@Aspect("perthis(call(* com.spazztv.epf.EPFImportTranslator..*(..)))")
public class EPFImportTranslatorLogger {

	public static long RECORD_LOG_COUNT = 5000;
	public static long TIMESTAMP_LOG_COUNT = 120000;

	private long lastLoggedRecord = 0;
	private long lastLoggedTimestamp = 0;
	private long totalExportedRecords = 0;

	private long recordsRead = 0;

	private String tableName;

	@After("    execution(* com.spazztv.epf.EPFImportTranslator.nextRecord(..)) && this(importTranslator)")
	public void afterNextRecord(EPFImportTranslator importTranslator) {
//		if (tableName == null) {
//			tableName = importTranslator.getTableName();
//		}
//		if (totalExportedRecords == 0) {
//			totalExportedRecords = importTranslator.getTotalExpectedRecords();
//			EPFImportTaskInfoBlock.getInstance().setTotalExportedRecords(
//					tableName, totalExportedRecords);
//		}
//		if (((importTranslator.getLastRecordRead() - lastLoggedRecord) > RECORD_LOG_COUNT)
//				|| ((System.currentTimeMillis() - lastLoggedTimestamp) > TIMESTAMP_LOG_COUNT)) {
//			Logger log = EPFImporter.getLogger();
//			log.info("{} - {} records imported",
//					importTranslator.getTableName(),
//					importTranslator.getLastRecordRead());
//			lastLoggedRecord = importTranslator.getLastRecordRead();
//			lastLoggedTimestamp = System.currentTimeMillis();
//		}
	}

//	@After("    execution(* com.spazztv.epf.adapter.SimpleEPFFileReader.nextDataRecord(..)) && this(fileReader)")
//	public void afterNextDataRecord(EPFFileReader fileReader) {
//		recordsRead++;
//		if (((recordsRead > RECORD_LOG_COUNT) || (System.currentTimeMillis() - lastLoggedTimestamp) > TIMESTAMP_LOG_COUNT)) {
//			Logger log = EPFImporter.getLogger();
//			if ((tableName != null) && (totalExportedRecords != 0)) {
//				log.info("{} - {} of {} records processed", tableName,
//						recordsRead, totalExportedRecords);
//			}
//		}
//	}
//
//	@After("    execution(* com.spazztv.epf.adapter.FilteredGamesEPFFileReader.nextDataRecord(..)) && this(fileReader)")
//	public void afterNextDataRecord2(EPFFileReader fileReader) {
//		recordsRead++;
//		if (((recordsRead > RECORD_LOG_COUNT) || (System.currentTimeMillis() - lastLoggedTimestamp) > TIMESTAMP_LOG_COUNT)) {
//			Logger log = EPFImporter.getLogger();
//			if ((tableName != null) && (totalExportedRecords != 0)) {
//				log.info("{} - {} of {} records processed", tableName,
//						recordsRead, totalExportedRecords);
//			}
//		}
//	}
}
