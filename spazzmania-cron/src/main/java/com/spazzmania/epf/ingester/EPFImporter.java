package com.spazzmania.epf.ingester;

import java.util.List;

import org.kohsuke.args4j.Option;

public class EPFImporter {

	@Option(name = "-f", aliases = "--flat", metaVar = "flat", usage = "\"Import EPF Flat files, using values from EPFFlat.config if not overridden\"")
	private boolean flat;
	@Option(name = "-r", aliases = "--resume", metaVar = "resume", usage = "\"Resume the most recent import according to the relevant .json status file (EPFStatusIncremental.json if -i, otherwise EPFStatusFull.json)\"")
	private boolean resume;
	@Option(name = "-d", aliases = "--dbhost", metaVar = "dbHost", usage = "\"The hostname of the database (default is localhost)\"")
	private String dbHost;
	@Option(name = "-u", aliases = "--dbuser", metaVar = "dbUser", usage = "\"The user which will execute the database commands; must have table create/drop priveleges\"")
	private String dbUser;
	@Option(name = "-p", aliases = "--dbpassword", metaVar = "dbPassword", usage = "\"The user's password for the database\"")
	private String dbPassword;
	@Option(name = "-n", aliases = "--dbname", metaVar = "dbName", usage = "\"The name of the database to connect to\"")
	private String dbName;
	@Option(name = "-s", aliases = "--recordseparator", metaVar = "recordSep", usage = "\"The string separating records in the file\"")
	private String recordSep;
	@Option(name = "-t", aliases = "--fieldseparator", metaVar = "fieldSep", usage = "\"The string separating fields in the file\"")
	private String fieldSep;
	@Option(name = "-a", aliases = "--allowextensions", metaVar = "allowExtensions", usage = "\"Include files with dots in their names in the import\"")
	private boolean allowExtensions;
	@Option(name = "-x", aliases = "--tableprefix", metaVar = "tablePrefix", usage = "\"Optional prefix which will be added to all table names, e.g. 'MyPrefix_video_translation'\"")
	private String tablePrefix;
	@Option(name = "-w", aliases = "--whitelist", metaVar = "<whiteList>", usage = "\"A regular expression to add to the whiteList; repeated -w arguments will append\"")
	private List<String> whiteList;
	@Option(name = "-b", aliases = "--blacklist", metaVar = "<blackList>", usage = "\"A regular expression to add to the whiteList; repeated -b arguments will append\"")
	private List<String> blackList;
	@Option(name = "-k", aliases = "--skipkeyviolators", metaVar = "skipKeyViolators", usage = "\"Ignore inserts which would violate a primary key constraint; only applies to full imports\"")
	private boolean skipKeyViolators;

	public static void printUsage() {
		System.out
				.println("usage: EPFDbImporter [-fxrak] [-d db_host] [-u db_user] [-p db_password] [-n db_name]");
		System.out
				.println("       [-s record_separator] [-t field_separator] [-w regex [-w regex2 [...]]] ");
		System.out
				.println("       [-b regex [-b regex2 [...]]] source_directory [source_directory2 ...]");
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
