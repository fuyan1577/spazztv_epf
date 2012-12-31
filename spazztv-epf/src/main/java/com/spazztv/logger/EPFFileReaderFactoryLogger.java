/**
 * 
 */
package com.spazztv.logger;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;

import com.spazztv.epf.EPFImporter;

/**
 * @author tjbillingsley
 *
 */
@Aspect
public class EPFFileReaderFactoryLogger {

	@AfterThrowing(pointcut = "call(* com.spazztv.epf.dao.EPFFileReaderFactory.getFileReader(..))", throwing = "error")
	public void afterThrowingExecuteNewReader(JoinPoint joinPoint, Throwable error) {
		Logger log = EPFImporter.getLogger();
		log.error("EPFFileReaderFactory: {}", error.getMessage());
	}
}
