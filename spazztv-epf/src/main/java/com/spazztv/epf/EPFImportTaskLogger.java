/**
 * 
 */
package com.spazztv.epf;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spazztv.epf.dao.EPFDbWriter;

/**
 * @author tjbillingsley
 *
 */
@Aspect("perthis(call(* com.spazztv.epf.EPFImportTask..*(..)))")
public class EPFImportTaskLogger {

	private EPFImportTranslator importTranslator;
	private Long totalExpectedRecords;
	
	public static String getLoggerClass() {
		return EPFImporter.class.getName();
	}

	@AfterReturning(pointcut="call(com.spazztv.epf.EPFImportTask+.new(..))",returning="importTask")
	public void newImportTask(EPFImportTask importTask) {
		importTranslator = importTask.getImportTranslator();
		String importFile = importTranslator.getFilePath();
		Logger log = LoggerFactory.getLogger(getLoggerClass());
		log.info("%s - EPF Import Task launching",importFile);
	}
	
    @After("    call(void com.spazztv.epf.EPFImportTask.setupImportDataStore(..)) ")
    public void afterSetupImportDataStore(JoinPoint joinPoint) {
    	Object importTask = joinPoint.getTarget();
    	importTranslator = ((EPFImportTask)importTask).getImportTranslator();
    	totalExpectedRecords = importTranslator.getTotalExpectedRecords();
		Logger log = LoggerFactory.getLogger(getLoggerClass());
        log.info("%s - Beggining import, total records to import: %d",importTranslator.getTableName(),totalExpectedRecords);
    }

    @After("    call(void com.spazztv.epf.EPFImportTask.finalizeImport(..)) ")
    public void afterFinalizeImport(JoinPoint joinPoint) {
    	Object importTask = joinPoint.getTarget();
    	EPFDbWriter dbWriter = ((EPFImportTask)importTask).getDbWriter();;
		Logger log = LoggerFactory.getLogger(getLoggerClass());
        log.info("%s - Import completed, %d of %d records imported",importTranslator.getTableName(),totalExpectedRecords,dbWriter.getTotalRowsInserted());
    }
}
