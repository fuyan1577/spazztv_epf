/**
 * 
 */
package com.spazzmania.cron;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.spazzmania.epf.importer.EPFImporterException;

/**
 * @author Thomas Billingsley
 * 
 */
public class ShellExecJob implements Job {
	public static String SHELL_EXECUTABLE = "executable";
	public static String SHELL_PARAMETERS = "parameters";
	public static String SHELL_DIRECTORY = "directory";

	//Command line arguments
	private String parameters;
	private String executable;
	private String directory;
	/**
	 * @return the parameters
	 */
	public String getParameters() {
		return parameters;
	}

	/**
	 * @return the executable
	 */
	public String getExecutable() {
		return executable;
	}

	/**
	 * @return the directory
	 */
	public String getDirectory() {
		return directory;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.quartz.Job#execute(org.quartz.JobExecutionContext)
	 */
	@Override
	public void execute(JobExecutionContext arg0) throws JobExecutionException {
		// TODO Auto-generated method stub
	}

	/**
	 * Loads and verifies the job settings set in the Quartz Configuration for
	 * this job.
	 * 
	 * @param context
	 *            - JobExectionContext (Quartz Scheduler)
	 */
	public void verifyJobDataMap(JobExecutionContext context) {
		JobDataMap dataMap = context.getJobDetail().getJobDataMap();
		if ((!dataMap.containsKey(SHELL_EXECUTABLE))
				|| (!dataMap.containsKey(SHELL_PARAMETERS))
				|| (!dataMap.containsKey(SHELL_DIRECTORY))) {
			throw new RuntimeException(
					"Invalid JobDataMap 'executable', 'parameters' and 'directory' must be provided");
		}
		executable = dataMap.getString(SHELL_EXECUTABLE);
		parameters = dataMap.getString(SHELL_PARAMETERS);
		directory = dataMap.getString(SHELL_DIRECTORY);
	}

	public void parseArgs(String[] args) throws ParseException, EPFImporterException {
		Options options = new Options();
		options.addOption("parameters",true,"Job Parameters");
		options.addOption("executable",true,"Job Executable");
		options.addOption("directory",true,"Base Directory");
		
		CommandLineParser parser = new PosixParser();
		// Get the command line arguments
		CommandLine line = parser.parse(options, args);
		
		for (Option option : line.getOptions()) {
			String arg = option.getLongOpt();
			if (arg.equals("parameters")) {
				this.parameters = line.getOptionValue(arg);
			}
			if (arg.equals("executable")) {
				this.executable = line.getOptionValue(arg);
			}
			if (arg.equals("directory")) {
				this.directory = line.getOptionValue(arg);
			}
		}
	}

	/**
	 * @param args
	 * @throws EPFImporterException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ParseException, EPFImporterException {
		ShellExecJob job = new ShellExecJob();
		job.parseArgs(args);
		try {
			job.execute(null);
		} catch (JobExecutionException e) {
			throw new RuntimeException(e);
		}
	}

}
