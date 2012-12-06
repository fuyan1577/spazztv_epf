/**
 * 
 */
package com.spazztv.epf;

import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;

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

	@After("    execution(* com.spazztv.epf.EPFImportTranslator.nextRecord(..)) && this(importTranslator)")
	public void afterNextRecord(EPFImportTranslator importTranslator) {
		Logger log = EPFImporter.getLogger();
		if (((importTranslator.getLastRecordRead() - lastLoggedRecord) > RECORD_LOG_COUNT)
				|| ((System.currentTimeMillis() - lastLoggedTimestamp) > TIMESTAMP_LOG_COUNT)) {
			log.info("%s - Import completed, %d of %d records imported",
					importTranslator.getTableName(), importTranslator.getTotalExpectedRecords(),
					importTranslator.getLastRecordRead());
		}
	}
}
