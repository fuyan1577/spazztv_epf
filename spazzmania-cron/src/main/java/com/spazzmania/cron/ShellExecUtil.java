/**
 * 
 */
package com.spazzmania.cron;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author billintj
 * 
 */
public class ShellExecUtil {
	private ProcessBuilder processBuilder;
	private Logger logger;
	private String shellExecutable;
	private String[] shellArguments;
	private File workingDirectory;

	/**
	 * @return the processBuilder
	 */
	public ProcessBuilder getProcessBuilder() {
		return processBuilder;
	}

	/**
	 * @param processBuilder the processBuilder to set
	 */
	public void setProcessBuilder(ProcessBuilder processBuilder) {
		this.processBuilder = processBuilder;
	}

	/**
	 * @return the logger
	 */
	public Logger getLogger() {
		return logger;
	}

	/**
	 * @param logger the logger to set
	 */
	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	/**
	 * @return the shellExecutable
	 */
	public String getShellExecutable() {
		return shellExecutable;
	}

	/**
	 * @param shellExecutable the shellExecutable to set
	 */
	public void setShellExecutable(String shellExecutable) {
		this.shellExecutable = shellExecutable;
	}

	/**
	 * @return the shellArguments
	 */
	public String[] getShellArguments() {
		return shellArguments;
	}

	/**
	 * @param shellArguments the shellArguments to set
	 */
	public void setShellArguments(String[] shellArguments) {
		this.shellArguments = shellArguments;
	}

	/**
	 * @return the workingDirectory
	 */
	public File getWorkingDirectory() {
		return workingDirectory;
	}

	/**
	 * @param workingDirectory the workingDirectory to set
	 */
	public void setWorkingDirectory(File workingDirectory) {
		this.workingDirectory = workingDirectory;
	}

	public ShellExecUtil(ProcessBuilder processBuilder, String shellExecutable,
			String[] shellArguments, File workingDirectory, Logger logger) {
		this.processBuilder = processBuilder;
		this.shellExecutable = shellExecutable;
		this.shellArguments = shellArguments;
		this.workingDirectory = workingDirectory;
		this.logger = logger;
	}
	
	public synchronized void start() {
		//Set up the process builder's directory and command arguments
		processBuilder.directory(workingDirectory);
		List<String> command = new ArrayList<String>();
		command.add(shellExecutable);
		for (int i = 0; i < shellArguments.length; i++) {
			command.add(shellArguments[i]);
		}
		processBuilder.command(command);
		processBuilder.redirectErrorStream(true);
		
		//Now - execute the command redirecting the output through the logger
		try {
			Process process = processBuilder.start();
			InputStream stdout = process.getInputStream ();
			BufferedReader reader = new BufferedReader (new InputStreamReader(stdout));
			String line;

			while ((line = reader.readLine ()) != null) {
			    logger.info(line);
			}
		} catch (IOException e) {
			System.out.println(e);
		}
	}
}
