/**
 * 
 */
package com.spazzmania.cron;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.spazzmania.epf.feed.EPFDataDownloadUtil;
import com.spazzmania.model.util.SpazzDBUtil;

/**
 * @author Thomas Billingsley
 * 
 */
public class SpazzDBCheck {

	@Option(name = "--username", metaVar = "<username>")
	private String username;

	@Option(name = "--password", metaVar = "<password>")
	private String password;

	@Option(name = "--host", metaVar = "<host>")
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

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 6) {
			throw new RuntimeException(
					"SpazzDBCheck --host=SPAZZDBURL --username=USERNAME --password=PASSWORD");
		}
		final SpazzDBCheck check = new SpazzDBCheck();
	    final CmdLineParser parser = new CmdLineParser(check);
	    try {
			parser.parseArgument(args);
		} catch (CmdLineException e) {
			throw new RuntimeException(e);
		}
	    System.out.println("SpazzDBCheck - Checking the connection to the Spazzmania DB...");
		check.run();
	}
}
