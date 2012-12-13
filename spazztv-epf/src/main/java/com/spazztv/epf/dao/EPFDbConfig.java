package com.spazztv.epf.dao;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Properties;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.spazztv.epf.EPFImporterException;

/**
 * This object is a simple pojo for holding datasbase configuration values for
 * use by the Database Connection Pool.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFDbConfig {
	public static String DB_CONNECTION_POOL = "dbConnection";
	public static String DB_DATA_SOURCE_CLASS = "dbDataSourceClass";
	public static String DB_DATA_SOURCE_OPTIONS = "dbDataSourceOptions";
	public static String DB_WRITER_CLASS = "dbWriterClass";
	public static String DB_DEFAULT_CATALOG = "dbDefaultCatalog";
	public static String DB_MIN_CONNECTIONS = "dbMinConnections";
	public static String DB_MAX_CONNECTIONS = "dbMaxConnections";
	public static String DB_URL = "dbUrl";
	public static String DB_USER = "dbUser";
	public static String DB_PASSWORD = "dbPassword";

	private String dbWriterClass;
	private String dbDataSourceClass;
	private Properties dbDataSourceOptions;
	private String defaultCatalog;
	private String dbUrl;
	private String username;
	private String password;
	private Integer minConnections = 1;
	private Integer maxConnections = 8;

	public EPFDbConfig() {
		dbDataSourceOptions = new Properties();
	}

	public EPFDbConfig(File configFile) throws IOException,
			EPFImporterException {
		this();
		String configJson = loadConfigFile(configFile);
		parseConfiguration(configJson);
	}

	@SuppressWarnings("unchecked")
	public void parseConfiguration(String configJson)
			throws EPFImporterException {
		JSONParser parser = new JSONParser();
		JSONObject connPoolObject;
		try {
			JSONObject json = (JSONObject) parser.parse(configJson);
			connPoolObject = (JSONObject) json.get(DB_CONNECTION_POOL);
		} catch (ParseException e) {
			throw new EPFImporterException(e.getMessage());
		}

		validDbConfig(connPoolObject);

		// Required Values
		dbWriterClass = verifyString(connPoolObject, DB_WRITER_CLASS);
		dbDataSourceClass = verifyString(connPoolObject, DB_DATA_SOURCE_CLASS);
		dbUrl = verifyString(connPoolObject, DB_URL);
		username = verifyString(connPoolObject, DB_USER);
		password = verifyString(connPoolObject, DB_PASSWORD);

		// Optional values
		defaultCatalog = (String) connPoolObject.get(DB_DEFAULT_CATALOG);
		minConnections = verifyIntegerDefault(connPoolObject,
				DB_MIN_CONNECTIONS, EPFDbConnector.DEFAULT_MIN_CONNECTIONS);
		maxConnections = verifyIntegerDefault(connPoolObject,
				DB_MAX_CONNECTIONS, EPFDbConnector.DEFAULT_MAX_CONNECTIONS);

		dbDataSourceOptions.clear();
		if (connPoolObject.containsKey(DB_DATA_SOURCE_OPTIONS)) {
			JSONObject dbDataSourceOptions = (JSONObject)connPoolObject.get(DB_DATA_SOURCE_OPTIONS);
			for (Object key : dbDataSourceOptions.keySet()) {
			    Object val = dbDataSourceOptions.get(key);
			    dbDataSourceOptions.put(key, val);
			}			
		}
	}

	private void validDbConfig(JSONObject configObj)
			throws EPFImporterException {
		for (Object key : configObj.keySet()) {
			if (!isValidConfigKey((String) key)) {
				throw new EPFImporterException("Invalid EPFConfig key: " + key);
			}
		}
	}

	public static boolean isValidConfigKey(String key) {
		if (key.equals(DB_CONNECTION_POOL) || key.equals(DB_WRITER_CLASS)
				|| key.equals(DB_DATA_SOURCE_CLASS)
				|| key.equals(DB_DEFAULT_CATALOG)
				|| key.equals(DB_MIN_CONNECTIONS)
				|| key.equals(DB_MAX_CONNECTIONS) || key.equals(DB_USER)
				|| key.equals(DB_URL) || key.equals(DB_PASSWORD)
				|| key.equals(DB_DATA_SOURCE_OPTIONS)) {
			return true;
		}
		return false;
	}

	private String verifyString(JSONObject connPoolObject, String key) {
		if (connPoolObject.get(key) == null) {
			throw new RuntimeException(String.format(
					"Missing EPFDbConnector Parameter: %s", key));
		}
		return (String) connPoolObject.get(key);
	}

	private Integer verifyIntegerDefault(JSONObject connPoolObject, String key,
			Integer defaultValue) {
		if (connPoolObject.get(key) == null) {
			return defaultValue;
		}
		Long val = (Long) connPoolObject.get(key);
		return (Integer) val.intValue();
	}

	private String loadConfigFile(File configFile) throws IOException {
		FileInputStream stream = new FileInputStream(configFile);
		try {
			FileChannel fc = stream.getChannel();
			MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0,
					fc.size());
			/* Instead of using default, pass in a decoder. */
			return Charset.defaultCharset().decode(bb).toString();
		} finally {
			stream.close();
		}
	}

	public String getDbWriterClass() {
		return dbWriterClass;
	}

	public void setDbWriterClass(String dbWriterClass) {
		this.dbWriterClass = dbWriterClass;
	}

	public String getDbDataSourceClass() {
		return dbDataSourceClass;
	}

	public void setDbDataSourceClass(String dbDataSourceClass) {
		this.dbDataSourceClass = dbDataSourceClass;
	}

	public String getDefaultCatalog() {
		return defaultCatalog;
	}

	public void setDefaultCatalog(String defaultCatalog) {
		this.defaultCatalog = defaultCatalog;
	}

	public Properties getDbDataSourceOptions() {
		return dbDataSourceOptions;
	}

	public void setDbDataSourceOptions(Properties dbDataSourceOptions) {
		this.dbDataSourceOptions = dbDataSourceOptions;
	}

	public String getDbUrl() {
		return dbUrl;
	}

	public void setDbUrl(String dbUrl) {
		this.dbUrl = dbUrl;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Integer getMinConnections() {
		return minConnections;
	}

	public void setMinConnections(Integer minConnections) {
		this.minConnections = minConnections;
	}

	public Integer getMaxConnections() {
		return maxConnections;
	}

	public void setMaxConnections(Integer maxConnections) {
		this.maxConnections = maxConnections;
	}
}
