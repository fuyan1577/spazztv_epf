package com.spazzmania.cron;

import java.io.IOException;

import org.apache.log4j.FileAppender;
import org.apache.log4j.SimpleLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Log4JTest1 {
	public static void main(String[] args) {
		SimpleLayout layout = new SimpleLayout();
		
		FileAppender appender = null;
		try {
			appender = new FileAppender(layout, "log/Log4JTest1.log", false);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// logger.setLevel((Level) Level.DEBUG);

		org.apache.log4j.Logger log4JLogger = org.apache.log4j.Logger.getLogger(Log4JTest1.class);
		log4JLogger.addAppender(appender);

		Logger logger = LoggerFactory.getLogger(Log4JTest1.class);
		logger.info("Hello World");
	}
}
