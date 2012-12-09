package com.spazztv.epf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogBackTest {
   public static void main(String[] args) {
      Logger log = LoggerFactory.getLogger(LogBackTest.class);
      log.info("Hello, World!");
      log.debug("Hello, Debug World!");
	  log.warn("Hello, Warning World!");
	  log.error("Hello, Error World!");
	  log.trace("Hello, Trace World!");
      for (int i = 0; i < 10; i++) {
    	  log.info(String.format("Hello Increment %d",i));
      }
   }
}
