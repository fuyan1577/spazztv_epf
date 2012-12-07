/**
 * 
 */
package com.spazztv.epf.adapter;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;

import com.spazztv.epf.EPFImporter;

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
		Object arg = joinPoint.getArgs()[0];
		String sqlStmt = (String)arg;
		Logger log = EPFImporter.getLogger();
		log.debug("MySQL Exec: " + sqlStmt);
	}
}
