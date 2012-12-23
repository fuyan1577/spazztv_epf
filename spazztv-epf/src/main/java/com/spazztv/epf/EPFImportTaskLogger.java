/**
 * 
 */
package com.spazztv.epf;

import java.util.Date;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;

import com.spazztv.epf.dao.EPFDbWriter;

/**
 * @author Thomas Billingsley
 * 
 */
@Aspect("perthis(call(* com.spazztv.epf.EPFImportTask..*(..)))")
public class EPFImportTaskLogger {

	private EPFImportTranslator importTranslator;
	private Long totalExpectedRecords;
	private Date startTimeStamp;

	@AfterReturning(pointcut = "call(com.spazztv.epf.EPFImportTask+.new(..))", returning = "importTask")
	public void newImportTask(EPFImportTask importTask) {
		importTranslator = importTask.getImportTranslator();
		String importFile = importTranslator.getFilePath();
		Logger log = EPFImporter.getLogger();
		log.info("{} - EPF Import Task launching", importFile);
	}

	@After("    call(void com.spazztv.epf.EPFImportTask.setupImportDataStore(..)) ")
	public void afterSetupImportDataStore(JoinPoint joinPoint) {
		startTimeStamp = new Date();
		Object importTask = joinPoint.getTarget();
		importTranslator = ((EPFImportTask) importTask).getImportTranslator();
		totalExpectedRecords = importTranslator.getTotalExpectedRecords();
		Logger log = EPFImporter.getLogger();
		log.info("{} - Beginning import, total records to import: {}",
				importTranslator.getTableName(), totalExpectedRecords);
	}

	@After("    call(void com.spazztv.epf.EPFImportTask.finalizeImport(..)) ")
	public void afterFinalizeImport(JoinPoint joinPoint) {
		Object importTask = joinPoint.getTarget();
		EPFDbWriter dbWriter = ((EPFImportTask) importTask).getDbWriter();
		importTranslator = ((EPFImportTask) importTask).getImportTranslator();
		totalExpectedRecords = importTranslator.getTotalExpectedRecords();
		Logger log = EPFImporter.getLogger();
		Date endTimeStamp = new Date();
		String elapsedTime = EPFImporterLogger.elapsedTimeFormat(endTimeStamp.getTime()
				- startTimeStamp.getTime());
		if (dbWriter.getTotalRowsInserted() == totalExpectedRecords) {
			log.info("{} - Import completed, {} of {} records imported",
					importTranslator.getTableName(),
					dbWriter.getTotalRowsInserted(), totalExpectedRecords);
		} else {
			log.error("{} - Import completed - MISSING RECORDS, Expecting {} - Actual {}",
					importTranslator.getTableName(),  totalExpectedRecords,
					dbWriter.getTotalRowsInserted());
		}
		log.info("{} - Import Task Elapsed Time: {}", importTranslator.getTableName(), elapsedTime);
	}
	
}
