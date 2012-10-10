package com.spazzmania.epf.dao;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.spazzmania.epf.importer.EPFImporterException;

/**
 * This object is a simple pojo for holding datasbase configuration values for
 * use by the Database Connection Pool.
 * 
 * @author Thomas Billingsley
 * 
 */
public class EPFDbConfig {
	public static String DB_CONNECTION_POOL = "dbConnection";
	public static String DB_DRIVER_CLASS = "dbDriverClass";
	public static String DB_DEFAULT_CATALOG = "dbDefaultCatalog";
	public static String DB_MIN_CONNECTIONS = "dbMinConnections";
	public static String DB_MAX_CONNECTIONS = "dbMaxConnections";
	public static String DB_URL = "dbUrl";
	public static String DB_USER = "dbUser";
	public static String DB_PASSWORD = "dbPassword";

	private String dbDriverClass;
	private String defaultCatalog;
	private String jdbcUrl;
	private String username;
	private String password;
	private Integer minConnections = 1;
	private Integer maxConnections = 8;

	public EPFDbConfig() {
		jdbcUrl = "jdbc:mysql://localhost:3306/epf";
		username="epfimporter";
		password="epf123";
	}

	public EPFDbConfig(File configFile) throws IOException,
			EPFImporterException {
		String configJson = loadConfigFile(configFile);
		parseConfiguration(configJson);
	}

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
		dbDriverClass = verifyString(connPoolObject, DB_DRIVER_CLASS);
		jdbcUrl = verifyString(connPoolObject, DB_URL);
		username = verifyString(connPoolObject, DB_USER);
		password = verifyString(connPoolObject, DB_PASSWORD);

		// Optional values
		defaultCatalog = (String) connPoolObject.get(DB_DEFAULT_CATALOG);
		minConnections = verifyIntegerDefault(connPoolObject, DB_MIN_CONNECTIONS,
				EPFDbConnector.DEFAULT_MIN_CONNECTIONS);
		maxConnections = verifyIntegerDefault(connPoolObject, DB_MAX_CONNECTIONS,
				EPFDbConnector.DEFAULT_MAX_CONNECTIONS);
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
		if (key.equals(DB_CONNECTION_POOL) || key.equals(DB_DRIVER_CLASS)
				|| key.equals(DB_DEFAULT_CATALOG)
				|| key.equals(DB_MIN_CONNECTIONS)
				|| key.equals(DB_MAX_CONNECTIONS) || key.equals(DB_USER)
				|| key.equals(DB_URL) || key.equals(DB_PASSWORD)) {
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
		return (Integer) connPoolObject.get(key);
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

	public String getDbDriverClass() {
		return dbDriverClass;
	}

	public void setJdbcDriverClass(String dbDriverClass) {
		this.dbDriverClass = dbDriverClass;
	}

	public String getDefaultCatalog() {
		return defaultCatalog;
	}

	public void setDefaultCatalog(String defaultCatalog) {
		this.defaultCatalog = defaultCatalog;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
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
