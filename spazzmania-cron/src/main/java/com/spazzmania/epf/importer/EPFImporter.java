package com.spazzmania.epf.importer;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.spazzmania.epf.dao.EPFDbConfig;

public class EPFImporter {

	public static String EPF_CONFIG_DEFAULT = "./EPFConfig.json";

	private EPFConfig config;
	private EPFDbConfig dbConfig;

	public static int EPF_PAUSE_INTERVAL;
	private int pauseInterval = EPF_PAUSE_INTERVAL;

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
		System.out
				.println("usage: EPFDbImporter [-fxrak] [-d db_host] [-u db_user] [-p db_password] [-n db_name]");
		System.out
				.println("       [-s record_separator] [-t field_separator] [-w regex [-w regex2 [...]]] ");
		System.out
				.println("       [-b regex [-b regex2 [...]]] source_directory [source_directory2 ...]");
	}

	private void updateConfigWithCommandLineArguments(CommandLine line) throws EPFImporterException {
		for (Option option : line.getOptions()) {
			String arg = option.getLongOpt();
			if (arg.equals("dburl")) {
				dbConfig.setJdbcUrl(line.getOptionValue(arg));
			} else if (arg.equals("dbuser")) {
				dbConfig.setUsername(line.getOptionValue(arg));
			} else if (arg.equals("dbpassword")) {
				dbConfig.setPassword(line.getOptionValue(arg));
			} else if (arg.equals("dbdriver")) {
				dbConfig.setJdbcDriverClass(line.getOptionValue(arg));
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
				config.setRecordSeparator(line.getOptionValue(EPFConfig.EPF_FLAT_RECORD_SEPARATOR_DEFAULT));
				config.setFieldSeparator(line.getOptionValue(EPFConfig.EPF_FLAT_FIELD_SEPARATOR_DEFAULT));
			}
		}
		
		for (String opt : line.getArgs()) {
			if (opt.contains("=")) {
				throw new EPFImporterException(String.format("Unrecognized Option: %s",opt));
			}
		}
	}

	public Options getOptions() {
		Options options = new Options();
		options.addOption("f", "flat", false,
				"\"Import EPF Flat files, using values from EPFFlat.config if not overridden\"");
		options.addOption(
				"r",
				"resume",
				false,
				"\"Resume the most recent import according to the relevant .json status file (EPFStatusIncremental.json if -i, otherwise EPFStatusFull.json);\"");
		options.addOption("h", "dbhost", true,
				"\"The hostname of the database (default is localhost);\"");
		options.addOption(
				"u",
				"dbuser",
				true,
				"\"The user which will execute the database commands; must have table create/drop priveleges\"");
		options.addOption("p", "dbpassword", true,
				"\"The user's password for the database\"");
		options.addOption("n", "dbname", true,
				"\"The name of the database to connect to\"");
		options.addOption("n", "dbname", true,
				"\"The name of the database to connect to\"");
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
		return options;
	}

	public EPFConfig loadConfig(String configPath) throws IOException,
			EPFImporterException {
		if (configPath == null) {
			configPath = EPF_CONFIG_DEFAULT;
		}

		return new EPFConfig(new File(configPath));
	}

	public EPFDbConfig loadDbConfig(String configPath) throws IOException,
			EPFImporterException {
		if (configPath == null) {
			configPath = EPF_CONFIG_DEFAULT;
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

		// Verify the configuration
		// verifyDbConfig();
	}

//	public void verifyDbConfig() throws EPFImporterException {
//		String msg = "";
//		if (dbConfig.getJdbcUrl() == null) {
//			msg += " - No JDBC URL Designated";
//		}
//		if (dbConfig.getUsername() == null) {
//			msg += " - No DB Username Designated";
//		}
//		if (dbConfig.getPassword() == null){
//			msg += " - No DB Password Designated";
//		}
//		if (msg.length() > 0) {
//			throw new EPFImporterException(msg);
//		}
//	}

	public void runImporterJob() {
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
	 */
	public static void main(String[] args) throws ParseException, IOException,
			EPFImporterException {
		if (args.length > 0) {
			EPFImporter importer = new EPFImporter();
			importer.parseCommandLine(args);
			importer.runImporterJob();
		} else {
			printUsage();
		}
	}
}
