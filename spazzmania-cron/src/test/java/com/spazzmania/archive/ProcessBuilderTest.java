package com.spazzmania.archive;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.regex.Pattern;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessBuilderTest {

	@Test
	public void test() {
		String os = System.getProperty("os.name");
        String[] command;
		if (Pattern.matches("[W|w]in.+",os)) {
	        command = new String[3];
	        command[0] = "cmd";
	        command[1] = "/C";
	        command[2] = "set";
		} else {
	        command = new String[2];
	        command[0] = "/bin/sh";
	        command[1] = "env";
		}
        ProcessBuilder builder = new ProcessBuilder();
		builder.redirectErrorStream(true);
		builder.command(command);
		String line = "";
		
		Logger logger = LoggerFactory.getLogger(this.getClass());
		
		try {
			Process process = builder.start();
			
			OutputStream stdin = process.getOutputStream ();
			InputStream stderr = process.getErrorStream ();
			InputStream stdout = process.getInputStream ();
			
			BufferedReader reader = new BufferedReader (new InputStreamReader(stdout));
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(stdin));
			
			while ((line = reader.readLine ()) != null) {
			    //System.out.println ("Stdout: " + line);
			    logger.info(line);
			}
		} catch (IOException e) {
			System.out.println(e);
		}
	}

}
