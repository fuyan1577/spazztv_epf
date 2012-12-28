/**
 * 
 */
package com.spazztv.logger;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.AfterThrowing;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.slf4j.Logger;

import com.spazztv.epf.EPFConfig;
import com.spazztv.epf.EPFImportManager;
import com.spazztv.epf.EPFImporter;
import com.spazztv.epf.dao.EPFDbConfig;

/**
 * Logger advice for the EPFImporter object.
 * <p>
 * This logger displays the configuration options under which the job will run.
 * The config file and command line options are logged.
 * 
 * @author Thomas Billingsley
 * 
 */
@Aspect
public class EPFImporterLogger {

	public static long startTimeStamp;

	// afterRunImporterJob - job is completed
	// @Before("execution(* myMotherOnThePhone(..)")
	// public void beforeSomeProcedure(JoinPoint joinPoint) {
	//
	// }

	@Before("call(* com.spazztv.epf.EPFImporter.parseCommandLine(..))")
	public void beforeParseCommandLine(JoinPoint joinPoint) {
		String[] args = (String[]) joinPoint.getArgs()[0];
		String msg = "";
		for (int i = 0; i < args.length; i++) {
			msg += args[i];
			if (i + 1 < args.length) {
				msg += " ";
			}
		}
		Logger log = EPFImporter.getLogger();
		log.info("EPFImporter Launched");
		log.info("EPFImporter command line args: {}", msg);
	}

	@Before("call(* com.spazztv.epf.EPFImporter.runImporterJob(..))")
	public void beforeRunningImporterJob(JoinPoint joinPoint) {
		startTimeStamp = new Date().getTime();
		EPFImporter epfImporter = (EPFImporter) joinPoint.getTarget();
		EPFConfig config = epfImporter.getConfig();
		EPFDbConfig dbConfig = epfImporter.getDbConfig();

		Logger log = EPFImporter.getLogger();
		log.info("EPF Config Properties:");
		log.info("  Whitelist");
		for (String itm : config.getWhiteList()) {
			log.info("    {}", itm);
		}
		log.info("  Blacklist");
		for (String itm : config.getBlackList()) {
			log.info("    {}", itm);
		}
		log.info("  Directory Paths");
		for (String itm : config.getDirectoryPaths()) {
			log.info("    {}", itm);
		}
		log.info("  Max Threads: {}", config.getMaxThreads());
		log.info("  Allow Extensions: {}", config.isAllowExtensions());
		log.info("  Skip Key Violators: {}", config.isSkipKeyViolators());
		log.info("  Table Prefix: {}", config.getTablePrefix());
		log.info("  Record Separator: {}", config.getRecordSeparator());
		log.info("  Field Separator: {}", config.getFieldSeparator());
		log.info("  Snapshot File: {}", config.getSnapShotFile());

		log.info("EPF DB Config Properties:");
		log.info("  DB Writer Class: {}", dbConfig.getDbWriterClass());
		log.info("  DB Data Source Class: {}", dbConfig.getDbDataSourceClass());
		log.info("  DB Default Catalog: {}", dbConfig.getDefaultCatalog());
		log.info("  DB URL: {}", dbConfig.getDbUrl());
		log.info("  DB Username: {}", dbConfig.getUsername());
		String pw = null;
		if (dbConfig.getPassword() != null) {
			pw = dbConfig.getPassword().replaceAll(".", "*");
		}
		log.info("  DB Password: {}", pw);
		log.info("  Min Connections: {}", dbConfig.getMinConnections());
		log.info("  Max Connections: {}", dbConfig.getMaxConnections());
	}

	@After("call(* com.spazztv.epf.EPFImporter.runImporterJob(..))")
	public void afterRunImporterJob(JoinPoint joinPoint) {
		long endTimeStamp = (new Date().getTime());

		String elapsedTime = EPFImporterLogger.elapsedTimeFormat(endTimeStamp
				- startTimeStamp);
		Logger log = EPFImporter.getLogger();
		log.info("EPFImporter Job Completed - Total Elapsed Time: {}", elapsedTime);
	}

	@AfterThrowing(pointcut = "execution(* com.spazztv.epf.EPFImporter.main(..))", throwing = "error")
	public void afterThrowingMainException(Throwable error) {
		Logger log = EPFImporter.getLogger();
		log.error("EPFImporter Error", error);
	}

	@After("call(* com.spazztv.epf.EPFImportManager.loadImportFileList(..)) && this(importManager)")
	public void afterLoadImportFileList(EPFImportManager importManager) {
		Logger log = EPFImporter.getLogger();
		log.info("EPFImporter Files To Be Imported...");
		if (importManager.getFileList().size() <= 0) {
			log.warn("  NO FILES FOUND FOR IMPORTING");
			return;
		}
		for (String file : importManager.getFileList()) {
			log.info("  {}", file);
		}
	}

	/**
	 * Converts a timestamp in milliseconds to DD HH:MM:SS.SSS
	 * 
	 * @param millis
	 * @return formatted elapsed time
	 */
	public synchronized static String elapsedTimeFormat(long millis) {
		long days = TimeUnit.MILLISECONDS.toDays(millis);
		millis -= TimeUnit.DAYS.toMillis(days);
		long hours = TimeUnit.MILLISECONDS.toHours(millis);
		millis -= TimeUnit.HOURS.toMillis(hours);
		long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
		millis -= TimeUnit.MINUTES.toMillis(minutes);
		long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
		millis -= TimeUnit.SECONDS.toMillis(seconds);
		String eTime = null;
		if (days > 0) {
			eTime = String.format("%d %02d:%02d:%02d.%03d", days, hours,
					minutes, seconds, millis);
		} else {
			eTime = String.format("%02d:%02d:%02d.%03d", hours, minutes,
					seconds, millis);
		}
		return eTime;
	}
}
