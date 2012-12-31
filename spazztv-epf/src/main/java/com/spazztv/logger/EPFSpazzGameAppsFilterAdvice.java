/**
 * 
 */
package com.spazztv.logger;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;

/**
 * Logger related advice to resets the application & genre_application totals
 * immediately after the FilteredGameAppsFilter as initialized.
 * 
 * @author Thomas Billingsley
 * 
 */
@Aspect
public class EPFSpazzGameAppsFilterAdvice {
	
	@After("    call(com.spazztv.epf.adapter.EPFSpazzGameAppsFilter.new(..)) ")
	public void resetReaderStats(JoinPoint joinPoint) {
		EPFImportTaskInfoBlock.getInstance().clear();
	}
	
}
