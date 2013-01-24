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

import com.spazzmania.epf.feed.EPFDataDownloadUtil;
import com.spazzmania.model.util.SpazzDBUtil;

/**
 * @author Thomas Billingsley
 * 
 */
public class SpazzDBCheck {

	//Command line arguments
	private String username;
	private String password;
	private String host;

	public void run() {
		SpazzDBUtil dbUtil = new SpazzDBUtil(host, "spazzmania", username,
				password);
		String lastDownloadDate = dbUtil
				.getKeyValue(EPFDataDownloadUtil.EPF_LAST_DOWNLOAD_DATE);
		if (lastDownloadDate != null) {
			System.out.println("Looks good...");
		}
		System.out.println(String.format("System Key %s = %s",
				EPFDataDownloadUtil.EPF_LAST_DOWNLOAD_DATE, lastDownloadDate));
	}
	
	public void parseArgs(String[] args) throws ParseException {
		Options options = new Options();
		options.addOption("parameters",true,"Job Parameters");
		options.addOption("executable",true,"Job Executable");
		options.addOption("directory",true,"Base Directory");
		
		CommandLineParser parser = new PosixParser();
		// Get the command line arguments
		CommandLine line = parser.parse(options, args);
		
		for (Option option : line.getOptions()) {
			String arg = option.getLongOpt();
			if (arg.equals("username")) {
				this.username = line.getOptionValue(arg);
			}
			if (arg.equals("password")) {
				this.password = line.getOptionValue(arg);
			}
			if (arg.equals("host")) {
				this.host = line.getOptionValue(arg);
			}
		}
		
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 6) {
			throw new RuntimeException(
					"SpazzDBCheck --host=SPAZZDBURL --username=USERNAME --password=PASSWORD");
		}
		final SpazzDBCheck check = new SpazzDBCheck();
	    try {
	    	check.parseArgs(args);
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	    System.out.println("SpazzDBCheck - Checking the connection to the Spazzmania DB...");
		check.run();
	}
}
