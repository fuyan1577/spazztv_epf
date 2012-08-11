package com.spazzmania.cron;

import java.io.IOException;

import org.apache.log4j.FileAppender;

public class Log4jMDCTest1 implements Runnable {

	private Integer threadNumber;
	private String threadName;
	private String jobName;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		for (int i = 1; i <= 2; i++) {
			Runnable runnable = new Log4jMDCTest1(new Integer(i));
			(new Thread(runnable)).start();
		}
	}

	public Log4jMDCTest1(Integer threadNumber) {
		this.threadNumber = threadNumber;
		threadName = "MDCThread-" + this.threadNumber.toString();
		jobName = "Log4jMDCTest-" + this.threadNumber.toString();

		//SimpleLayout layout = new SimpleLayout();
		org.apache.log4j.PatternLayout layout = new org.apache.log4j.PatternLayout("%-5p | %d %t %c: %m%n");

		FileAppender appender = null;
		try {
			appender = new FileAppender(layout, "log/" + threadName + ".log",
					false);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// logger.setLevel((Level) Level.DEBUG);
		
		org.apache.log4j.Logger logger = org.apache.log4j.Logger
				.getLogger(jobName);
		//logger.setLevel((Level) Level.INFO);
		logger.addAppender(appender);
	}

	public void run() {
		Thread thread = Thread.currentThread();
		thread.setName(threadName);

		// Logger logger = LoggerFactory.getLogger(jobName);
		org.apache.log4j.Logger logger = org.apache.log4j.Logger
				.getLogger(jobName);
		for (int i = 0; i < 10; i++) {
			logger.info("Hello from MDC Test Step: " + jobName);
		}
	}
}
