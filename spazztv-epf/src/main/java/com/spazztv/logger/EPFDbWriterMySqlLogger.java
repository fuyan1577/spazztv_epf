/**
 * 
 */
package com.spazztv.logger;

import java.util.List;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;

import com.spazztv.epf.EPFImporter;
import com.spazztv.epf.adapter.EPFDbWriterMySqlDao;
import com.spazztv.epf.adapter.SQLReturnStatus;
import com.spazztv.epf.dao.EPFDbWriter;

/**
 * Logger advice for EPFDbWriterMySql (EPFDbWriterMySqlDao)
 * <p>
 * All logging defined here is debug level only which outputs verbose SQL
 * statements.
 * 
 * <p>
 * Log level debugging is controlled by the corresponding log4j.xml
 * configuration.
 * 
 * @author Thomas Billingsley
 * 
 */
@Aspect("perthis(call(* com.spazztv.epf.adapter.EPFDbWriterMySqlDao..*(..)))")
public class EPFDbWriterMySqlLogger {

	@Before("call(* com.spazztv.epf.adapter.EPFDbWriterMySqlDao.executeSQLStatement(..))")
	public void beforeExecuteSQLStatement(JoinPoint joinPoint) {
		Logger log = EPFImporter.getLogger();
		if (log.isDebugEnabled()) {
			log.debug("MySQL Exec: " + joinPoint.getArgs()[0]);
		}
	}
	
	@Before("execution(* com.spazztv.epf.adapter.EPFDbWriterMySql.insertRow(..)) && this(dbWriter)")
	public void beforeInsertRow(JoinPoint joinPoint, EPFDbWriter dbWriter) {
		@SuppressWarnings("unchecked")
		List<String> rowData = (List<String>)joinPoint.getArgs()[0];
		if (rowData != null) {
			if (dbWriter.getColumnsAndTypes().size() != rowData.size()) {
				Logger log = EPFImporter.getLogger();
				log.error("Invalid import record #{}. Expected columns {}, Actual {}", dbWriter.getTotalRowsInserted() + 1, dbWriter.getColumnsAndTypes().size(), rowData.size());
			}
		}
	}
	
	@AfterThrowing(pointcut = "call(* com.spazztv.epf.adapter.EPFDbWriterMySql.executeSQLStatementWithRetry(..))", throwing = "error")
	public void afterThrowingExecuteSqlStatementWithRetry(JoinPoint joinPoint, Throwable error) {
		String sqlStmt = (String)joinPoint.getArgs()[0];
		Logger log = EPFImporter.getLogger();
		log.error("MySql Statement: {}", sqlStmt);
		log.error("MySql Error", error);
	}
	
	@AfterReturning(pointcut = "call(* com.spazztv.epf.adapter.EPFDbWriterMySqlDao.executeSQLStatement(..))", returning = "sqlStatus")
	public void afterExecuteSQLStatement(JoinPoint joinPoint, SQLReturnStatus sqlStatus) {
		if (sqlStatus.getDescription() != null) {
			@SuppressWarnings("unchecked")
			int i = ((List<List<String>>)joinPoint.getArgs()[1]).size();
			if (i == 1) {
				String tableName = ((EPFDbWriterMySqlDao)joinPoint.getTarget()).getTableName();
				Logger log = EPFImporter.getLogger();
				log.warn("{} - MySql Error: {}", tableName, sqlStatus.getDescription());
			}
		}
	}
}
