package com.spazztv.epf;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spazztv.epf.dao.EPFDbConfig;
import com.spazztv.epf.dao.EPFDbException;

public class EPFImporter {

	private EPFConfig config;
	private EPFDbConfig dbConfig;

	public static int EPF_PAUSE_INTERVAL = 10000;
	private int pauseInterval = EPF_PAUSE_INTERVAL;
	
	private static Logger logger;
	
	public EPFConfig getConfig() {
		return config;
	}

	public void setConfig(EPFConfig config) {
		this.config = config;
	}

	public EPFDbConfig getDbConfig() {
		return dbConfig;
	}

	public void setDbConfig(EPFDbConfig dbConfig) {
		this.dbConfig = dbConfig;
	}

	public void setPauseInterval(int pauseInterval) {
		this.pauseInterval = pauseInterval;
	}

	public static void printUsage() {
//		EPFImporter [ Options: [ short {f=[ option: f field_separator  [ARG] :: "The field separator" ], d=[ option: d db_config  [ARG] :: "The configuration file path and name. Defaults to EPFConfig.json in the local directory" ], b=[ option: b blacklist  [ARG] :: "A regular expression to add to the whiteList; repeated -b arguments will append" ], c=[ option: c config  [ARG] :: "The configuration file path and name. Defaults to EPFConfig.json in the local directory" ], a=[ option: a allowextensions  :: "Include files with dots in their names in the import" ], l=[ option: l dbdriver  [ARG] :: "The JDBC Driver Class name" ], k=[ option: k skipkeyviolators  :: "Ignore inserts which would violate a primary key constraint; only applies to full imports" ], dburl=[ option: dburl  [ARG] :: "The name of the database to connect to" ], dbwriter=[ option: dbwriter  [ARG] :: "The JDBC DataSource Class name" ], w=[ option: w whitelist  [ARG] :: "A regular expression to add to the whiteList; repeated -w arguments will append" ], u=[ option: u dbuser  [ARG] :: "The user which will execute the database commands; must have table create/drop priveleges" ], t=[ option: t max_threads  [ARG] :: "The maximum concurrently executing threads. Default to 8" ], s=[ option: s snapshot  [ARG] :: "The snapshot file for allowing restarts" ], r=[ option: r record_separator  [ARG] :: "The record separator" ], p=[ option: p dbpassword  [ARG] :: "The user's password for the database" ], x=[ option: x tableprefix  [ARG] :: "Optional prefix which will be added to all table names, e.g. 'MyPrefix_video_translation'" ]} ] [ long {dbdriver=[ option: l dbdriver  [ARG] :: "The JDBC Driver Class name" ], allowextensions=[ option: a allowextensions  :: "Include files with dots in their names in the import" ], db_config=[ option: d db_config  [ARG] :: "The configuration file path and name. Defaults to EPFConfig.json in the local directory" ], snapshot=[ option: s snapshot  [ARG] :: "The snapshot file for allowing restarts" ], tableprefix=[ option: x tableprefix  [ARG] :: "Optional prefix which will be added to all table names, e.g. 'MyPrefix_video_translation'" ], flat=[ option: f flat  :: "Import EPF Flat files, using values from EPFFlat.config if not overridden" ], dbuser=[ option: u dbuser  [ARG] :: "The user which will execute the database commands; must have table create/drop priveleges" ], field_separator=[ option: f field_separator  [ARG] :: "The field separator" ], resume=[ option: r resume  :: "Resume the most recent import according to the relevant .json status file (EPFStatusIncremental.json if -i, otherwise EPFStatusFull.json);" ], dbpassword=[ option: p dbpassword  [ARG] :: "The user's password for the database" ], blacklist=[ option: b blacklist  [ARG] :: "A regular expression to add to the whiteList; repeated -b arguments will append" ], config=[ option: c config  [ARG] :: "The configuration file path and name. Defaults to EPFConfig.json in the local directory" ], record_separator=[ option: r record_separator  [ARG] :: "The record separator" ], max_threads=[ option: t max_threads  [ARG] :: "The maximum concurrently executing threads. Default to 8" ], whitelist=[ option: w whitelist  [ARG] :: "A regular expression to add to the whiteList; repeated -w arguments will append" ], skipkeyviolators=[ option: k skipkeyviolators  :: "Ignore inserts which would violate a primary key constraint; only applies to full imports" ]} ]		
		
		System.out
				.println("usage: EPFDbImporter [-fxrak] [-d db_host] [-u db_user] [-p db_password] [-n db_name]");
		System.out
				.println("       [-s record_separator] [-t field_separator] [-w regex [-w regex2 [...]]] ");
		System.out
				.println("       [-b regex [-b regex2 [...]]] source_directory [source_directory2 ...]");
	}
	
	public static Logger getLogger() {
		if (logger == null) {
			logger = LoggerFactory.getLogger(EPFImporter.class.getName());
		}
		return logger;
	}

	private void updateConfigWithCommandLineArguments(CommandLine line)
			throws EPFImporterException {
		for (Option option : line.getOptions()) {
			String arg = option.getOpt();
			if (option.getLongOpt() != null) {
				arg = option.getLongOpt();
			}
			if (arg.equals("dburl")) {
				dbConfig.setDbUrl(line.getOptionValue(arg));
			} else if (arg.equals("dbuser")) {
				dbConfig.setUsername(line.getOptionValue(arg));
			} else if (arg.equals("dbpassword")) {
				dbConfig.setPassword(line.getOptionValue(arg));
			} else if (arg.equals("dbdriver")) {
				dbConfig.setDbDataSourceClass(line.getOptionValue(arg));
			} else if (arg.equals("dbwriter")) {
				dbConfig.setDbWriterClass(line.getOptionValue(arg));
			} else if (arg.equals("allowextensions")) {
				config.setAllowExtensions(true);
			} else if (arg.equals("tableprefix")) {
				config.setTablePrefix(line.getOptionValue(arg));
			} else if (arg.equals("whitelist")) {
				config.setWhiteList(Arrays.asList(line.getOptionValues(arg)));
			} else if (arg.equals("blacklist")) {
				config.setBlackList(Arrays.asList(line.getOptionValues(arg)));
			} else if (arg.equals("skipkeyviolators")) {
				config.setSkipKeyViolators(true);
			} else if (arg.equals("recordseparator")) {
				config.setRecordSeparator(line.getOptionValue(arg));
			} else if (arg.equals("fieldseparator")) {
				config.setFieldSeparator(line.getOptionValue(arg));
			} else if (arg.equals("flat")) {
				config.setRecordSeparator(line
						.getOptionValue(EPFConfig.EPF_FLAT_RECORD_SEPARATOR_DEFAULT));
				config.setFieldSeparator(line
						.getOptionValue(EPFConfig.EPF_FLAT_FIELD_SEPARATOR_DEFAULT));
			}
		}

		for (String opt : line.getArgs()) {
			if (opt.contains("=")) {
				throw new EPFImporterException(String.format(
						"Unrecognized Option: %s", opt));
			}
		}

		// All additional arguments are directory paths for locating input files
		config.setDirectoryPaths(Arrays.asList(line.getArgs()));
	}

	public static Options getOptions() {
		Options options = new Options();
		options.addOption("f", "flat", false,
				"\"Import EPF Flat files, using values from EPFFlat.config if not overridden\"");
		options.addOption(
				"r",
				"resume",
				false,
				"\"Resume the most recent import according to the relevant .json status file (EPFStatusIncremental.json if -i, otherwise EPFStatusFull.json);\"");
		// options.addOption("h", "dbhost", true,
		// "\"The hostname of the database (default is localhost);\"");
		options.addOption(
				"u",
				"dbuser",
				true,
				"\"The user which will execute the database commands; must have table create/drop priveleges\"");
		options.addOption("p", "dbpassword", true,
				"\"The user's password for the database\"");
		options.addOption("dburl", true,
				"\"The name of the database to connect to\"");
		options.addOption("l", "dbdriver", true,
				"\"The JDBC Driver Class name\"");
		options.addOption("dbwriter", true,
				"\"The JDBC DataSource Class name\"");
		options.addOption("t", "max_threads", true,
				"\"The maximum concurrently executing threads. Default to 8\"");
		options.addOption("a", "allowextensions", false,
				"\"Include files with dots in their names in the import\"");
		options.addOption(
				"x",
				"tableprefix",
				true,
				"\"Optional prefix which will be added to all table names, e.g. 'MyPrefix_video_translation'\"");
		options.addOption(
				"w",
				"whitelist",
				true,
				"\"A regular expression to add to the whiteList; repeated -w arguments will append\"");
		options.addOption(
				"b",
				"blacklist",
				true,
				"\"A regular expression to add to the whiteList; repeated -b arguments will append\"");
		options.addOption(
				"k",
				"skipkeyviolators",
				false,
				"\"Ignore inserts which would violate a primary key constraint; only applies to full imports\"");
		options.addOption(
				"c",
				"config",
				true,
				"\"The configuration file path and name. Defaults to EPFConfig.json in the local directory\"");
		options.addOption(
				"d",
				"db_config",
				true,
				"\"The configuration file path and name. Defaults to EPFConfig.json in the local directory\"");
		options.addOption("r", "record_separator", true,
				"\"The record separator\"");
		options.addOption("f", "field_separator", true,
				"\"The field separator\"");
		options.addOption("s", "snapshot", true,
				"\"The snapshot file for allowing restarts\"");
		return options;
	}

	public void verifyConfiguration() throws EPFImporterException {
		boolean invalidConfig = false;
		String msg = "";
		if (dbConfig.getDbDataSourceClass() == null) {
			invalidConfig = true;
		}
		if (dbConfig.getDbUrl() == null) {
			invalidConfig = true;
			msg += " - JDBC URL Connection string is required";
		}
		if (dbConfig.getUsername() == null) {
			invalidConfig = true;
			msg += " - JDBC Username is required";
		}
		if (dbConfig.getUsername() == null) {
			invalidConfig = true;
			msg += " - JDBC Password is required";
		}
		if (config.getDirectoryPaths() == null) {
			invalidConfig = true;
			msg += " - Input directory path is required";
		}
		if (invalidConfig) {
			System.out.println("Invalid options/configuration:");
			throw new EPFImporterException(msg);
		}
	}

	public EPFConfig loadConfig(String configPath) throws IOException,
			EPFImporterException {
		if (configPath == null) {
			return null;
		}

		return new EPFConfig(new File(configPath));
	}

	public EPFDbConfig loadDbConfig(String configPath) throws IOException,
			EPFImporterException {
		if (configPath == null) {
			return null;
		}

		return new EPFDbConfig(new File(configPath));
	}

	/**
	 * Parse the command line arguments and set the values in EPFConfig and
	 * EPFDbConfig.
	 * 
	 * <p/>
	 * The default configuration file for both EPFConfig and EPFDbConfig are the
	 * same file, <i>EPFConfig.json</i>.
	 * <p/>
	 * Config files are loaded first. Any specified command line options
	 * override the values in the JSON configuration files.
	 * 
	 * @param args
	 */
	public void parseCommandLine(String[] args) throws ParseException,
			IOException, EPFImporterException {
		CommandLineParser parser = new PosixParser();
		// Get the command line arguments
		CommandLine line = parser.parse(getOptions(), args);
		
		// Load the configurations
		String configFile = line.getOptionValue("config");
		if (configFile != null) {
			config = loadConfig(configFile);
		} else {
			config = new EPFConfig();
		}
		String dbConfigFile = line.getOptionValue("db_config");
		if (line.hasOption("db_config")) {
			dbConfigFile = line.getOptionValue("db_config");
		}
		if (dbConfigFile != null) {
			dbConfig = loadDbConfig(dbConfigFile);
		} else {
			dbConfig = new EPFDbConfig();
		}

		// Override the configurations with any specified command line args
		updateConfigWithCommandLineArguments(line);
	}

	public void runImporterJob() throws EPFDbException {
		EPFImportManager importManager = new EPFImportManager(config, dbConfig);
		// Wait for the EPFImportManager to complete
		// Check every 60 seconds if any threads are still executing

		while (true) {
			if (!importManager.isRunning()) {
				break;
			}
			try {
				Thread.sleep(pauseInterval);
			} catch (InterruptedException e) {
				// Ignore
			}
		}
	}

	/**
	 * @param args
	 * @throws EPFImporterException
	 * @throws IOException
	 * @throws ParseException
	 * @throws EPFDbException
	 */
	public static void main(String[] args) throws ParseException, IOException,
			EPFImporterException, EPFDbException {
		if (args.length > 0) {
			EPFImporter importer = new EPFImporter();
			importer.parseCommandLine(args);
			importer.verifyConfiguration();
			importer.runImporterJob();
		} else {
			printUsage();
		}
	}
}
